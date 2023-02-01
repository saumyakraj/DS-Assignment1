#ISSUES:
#1. ConsumerConsume works one by one
#2. 1Producer is managing multiple topics

from django.shortcuts import render
from rest_framework import generics, serializers, status, views, permissions
from rest_framework.response import Response

import threading
from threading import Lock
import traceback
from .models import Topic, Producer, Consumer, Message

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
            return Response(data={"status": "failure", "message": "Content-Type not supported"}, status=status.HTTP_400_BAD_REQUEST)
        
        topic_name = None
        try:
            receive = request.data
            topic_name = receive['topic_name']
        except:
            return Response(data={"status": "failure", "message": "Error While Parsing json"}, status=status.HTTP_400_BAD_REQUEST)        
                
        try:
            if Topic.objects.filter(name=topic_name).first() is not None:
                return Response(data={"status": "failure", "message": "Topic already exists"}, status=status.HTTP_400_BAD_REQUEST)        
            topic = Topic(name=topic_name)
            topic.save()

        except:
            return Response(data={"status": "failure", "message": "Error while querying/comitting to database"}, status=status.HTTP_400_BAD_REQUEST)        

        
        return Response(data={"status": "success", "message": 'topic ' + topic.name + ' created sucessfully'}, status=status.HTTP_200_OK)



    def get(self, request):
        print_thread_id()
        topics_name = []
        try:
            topics = Topic.objects.all()
            for t in topics:
                topics_name.append(t.name)
            return Response(data={"status": "success", "message": topics_name}, status=status.HTTP_200_OK)

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
            topic_name = receive['topic']
        except:
            return Response(data={"status": "failure", "message": "error while parsing request"}, status=status.HTTP_400_BAD_REQUEST)

            # topic can't be deleted, no need to lock queues
        # query
        try:
            topic = Topic.objects.filter(name=topic_name).first()
            if topic is None:
                return Response(data={"status": "failure", "message": "Topic does not exist"}, status=status.HTTP_400_BAD_REQUEST)

            producer = Producer(topic_id=topic)
            producer.save()
            return Response(data={"status": "success", "producer_id": str(Producer(producer)).id}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response(data={"status": "failure", "message": "error while querying/commiting database", "e": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class ConsumerRegister(views.APIView):

    def post(self, request):
        print_thread_id()
        content_type = request.headers.get('Content-Type')
        if content_type != 'application/json':
            return Response(data={"status": "failure", "message": "Content-Type not supported"}, status=status.HTTP_400_BAD_REQUEST)
        
        topic = None
        try:
            receive = request.data
            topic_name = receive['topic']
        except:
            return Response(data={"status": "failure", "message": "error while parsing request"}, status=status.HTTP_400_BAD_REQUEST)

        # query
        try:
            topic = Topic.objects.filter(name=topic_name).first()
            if topic is None:
                return Response(data={"status": "failure", "message": "Topic does not exist"}, status=status.HTTP_400_BAD_REQUEST)

            consumer = Consumer(topic_id=topic, offset=-1)
            consumer.save()
            return Response(data={"status": "success", "consumer_id": "consumer.id"}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response(data={"status": "failure", "message": "Error while querying/commiting database", "e": str(e)}, status=status.HTTP_400_BAD_REQUEST)


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
