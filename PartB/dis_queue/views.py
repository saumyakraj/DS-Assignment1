#ISSUES:
#1. ConsumerConsume works one by one


from django.shortcuts import render
from rest_framework import generics, serializers, status, views, permissions
from rest_framework.response import Response

import threading
from threading import Lock
import traceback

# queue data structures

# queue for each topic
class TopicQueue:
    def __init__(self, topic_name):
        # each topic queue has its own lock to ensure broker ordering
        self.lock = Lock()
        self.messages = []
        self.topic_name = topic_name

# stores producer information
class Producers:
    def __init__(self):
        self.count = 0 # count for assigning producer_id
        self.lock = Lock() # lock for getting producer_id
        self.topics = dict() # stores topic: producer_id

class Consumers:
    def __init__(self):
        self.count = 0 # count for assigning consumer_id
        self.lock = Lock() # lock for getting consumer_id
        self.topics = dict() # stores topic: consumer_id
        self.offsets = dict() # stores message offset for each consumer_id

queues_lock = Lock()
queues = dict()

# producer data structures
producers = Producers()
consumers = Consumers()


# debugging functions
def print_thread_id():
    print('Request handled by worker thread:', threading.get_native_id())

class Topics(views.APIView):

    def post(self, request):
        print_thread_id()
        content_type = request.headers.get('Content-Type')
        if content_type != 'application/json':
            return Response(data={"status": "failure", "message": "Content-Type not supported"}, status=status.HTTP_401_UNAUTHORIZED)
        
        topic_name = None
        try:
            # print ("25")
            receive = request.data
            topic_name = receive['topic_name']
        except:
            return Response(data={"status": "failure", "message": "Error While Parsing json"}, status=status.HTTP_401_UNAUTHORIZED)        
                
        # lock the queues, we don't want to return the wrong status
        # amd perform an unecessary insert
        global queues
        global queues_lock
        with queues_lock:
            if topic_name not in queues:
                queues[topic_name] = TopicQueue(topic_name)
                return Response(data={"status": "success", "message": 'topic ' + topic_name + ' created sucessfully'}, status=status.HTTP_200_OK)
            else: 
                return Response(data={"status": "failure", "message": "Topic already exists"}, status=status.HTTP_401_UNAUTHORIZED)
    
        # this is not dead code, this can return if there's an exception in queues_lock
        return Response(data={"status": "failure", "message": "Error while aquiring queue lock"}, status=status.HTTP_401_UNAUTHORIZED)

    def get(self, request):
        print_thread_id()
        topics = []
        try:
            global queues  
            # no need to lock queues, topics can't be deleted
            for key in queues:
                topics.append(key)
            return Response(data={"status": "success", "message": topics}, status=status.HTTP_200_OK)

        except: 
            return Response(data={"status": "failure", "message": "Error while listing topics"}, status=status.HTTP_400_BAD_REQUEST)

class ProducerRegister(views.APIView):

    def post(self, request):
        print_thread_id()
        content_type = request.headers.get('Content-Type')
        if content_type != 'application/json':
            return Response(data={"status": "failure", "message": "Content-Type not supported"}, status=status.HTTP_400_BAD_REQUEST)
        
        topic = None
        try:
            receive = request.data
            topic = receive['topic']
        except:
            return Response(data={"status": "failure", "message": "error while parsing request"}, status=status.HTTP_400_BAD_REQUEST)

            # topic can't be deleted, no need to lock queues
        if topic not in queues:
            return Response(data={"status": "failure", "message": "Topic not found"}, status=status.HTTP_400_BAD_REQUEST)
        
        global producers
        new_id = -1
        with producers.lock:
            new_id = producers.count
            producers.count += 1
            producers.topics[new_id] = topic
        
        if new_id == -1:
            return Response(data={"status": "failure", "message": "Can not assign new id"}, status=status.HTTP_400_BAD_REQUEST)
            
        return Response(data={"status": "success", "producer_id": new_id}, status=status.HTTP_200_OK)
    
class ConsumerRegister(views.APIView):

    def post(self, request):
        print_thread_id()
        content_type = request.headers.get('Content-Type')
        if content_type != 'application/json':
            return Response(data={"status": "failure", "message": "Content-Type not supported"}, status=status.HTTP_400_BAD_REQUEST)
        
        topic = None
        try:
            receive = request.data
            topic = receive['topic']
        except:
            return Response(data={"status": "failure", "message": "error while parsing request"}, status=status.HTTP_400_BAD_REQUEST)

        global consumers
        new_id = -1
        with consumers.lock:
            new_id = consumers.count
            consumers.count += 1
            consumers.topics[new_id] = topic
            # maintain a seperate lock for each consumer offset
            consumers.offsets[new_id] = [0, Lock()]
        
        if new_id == -1:
            return Response(data={"status": "failure", "message": "Can not assign new id"}, status=status.HTTP_400_BAD_REQUEST)

        return Response(data={"status": "success", "consumer_id": new_id}, status=status.HTTP_200_OK)


class ProducerProduce(views.APIView):

    def post(self, request):
        print_thread_id()
        content_type = request.headers.get('Content-Type')
        if content_type != 'application/json':
            return Response(data={"status": "failure", "message": "Content-Type not supported"}, status=status.HTTP_400_BAD_REQUEST)
        
        topic = None
        producer_id = None
        message = None
        try:
            receive = request.data
            topic = receive['topic']
            producer_id = receive['producer_id']
            message = receive['message']
        except:
            return Response(data={"status": "failure", "message": "error while parsing request"}, status=status.HTTP_400_BAD_REQUEST)
        
        global producers
        if producer_id not in producers.topics:
            return Response(data={"status": "failure", "message": "producer_id does not exist"}, status=status.HTTP_400_BAD_REQUEST)
        
        if producers.topics[producer_id] != topic:
            return Response(data={"status": "failure", "message": "topic does not match for given producer_id"}, status=status.HTTP_400_BAD_REQUEST)

        
        # lock queue for requested topic
        with queues[topic].lock:
            queues[topic].messages.append(message)
        
        return Response(data={"status": "success"}, status=status.HTTP_200_OK)
        

class ConsumerConsume(views.APIView):

    def get(self, request):
        print_thread_id()   
        try:
            topic = request.data['topic']
            consumer_id = request.data['consumer_id']
            consumer_id = int(consumer_id)
        except Exception as e:
            return Response(data={"status": "failure", "message": "Error While Parsing json"}, status=status.HTTP_400_BAD_REQUEST)

            
        global consumers
        if consumer_id not in consumers.topics:
            return Response(data={"status": "failure", "message": "consumer_id does not exist"}, status=status.HTTP_400_BAD_REQUEST)

        if consumers.topics[consumer_id] != topic:
            return Response(data={"status": "failure", "message": "topic does not match for given consumer_id"}, status=status.HTTP_400_BAD_REQUEST)

        
        # retreive message
        message = None
        with consumers.offsets[consumer_id][1]:
            try:
                message = queues[topic].messages[consumers.offsets[consumer_id][0]]
                consumers.offsets[consumer_id][0] += 1
            except:
                return Response(data={"status": "failure", "message": "no more logs"}, status=status.HTTP_400_BAD_REQUEST)
            
        return Response(data={"status": "success", "message": message}, status=status.HTTP_200_OK)
        

class Size(views.APIView):

    def get(self, request):
        print_thread_id()   
        try:
            topic = request.data['topic']
            consumer_id = request.data['consumer_id']
            consumer_id = int(consumer_id)
        except:
            return Response(data={"status": "failure", "message": "error while parsing request"}, status=status.HTTP_400_BAD_REQUEST)

            
        global consumers
        if consumer_id not in consumers.topics:
            return Response(data={"status": "failure", "message": "consumer_id does not exist"}, status=status.HTTP_400_BAD_REQUEST)

        if consumers.topics[consumer_id] != topic:
            return Response(data={"status": "failure", "message": "topic does not match for given consumer_id"}, status=status.HTTP_400_BAD_REQUEST)
        
        
        messages_left = 0
        try:
            messages_left = len(queues[topic].messages) - consumers.offsets[consumer_id][0]
        except:
            return Response(data={"status": "failure", "message": "an error occured"}, status=status.HTTP_400_BAD_REQUEST)


        return Response(data={"status": "success", "size": messages_left}, status=status.HTTP_200_OK)
