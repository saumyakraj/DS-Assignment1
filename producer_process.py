from Library.producer import Producer

import random

import sys
import time

max_timeout = 0.01
name = sys.argv[1]
topics = sys.argv[2]
log_filename = sys.argv[3]

topics = topics.split(',')

log_file = open(log_filename, 'r')

logs = log_file.readlines()

# run using sdk
producer = Producer('localhost', 5000, name)
for t in topics:
    print("reachedtopic itr")
    producer.register(t)
print("reached1")
# file for logging productions (helpful in verification)
file = open('log/' + producer.name + '.txt', 'w')
print("reached2")


print(producer.name, 'Starting...')
count = 0
for line in logs:
    count += 1
    print(count)
    # print("reached3.1")
    tokens = line.strip().split("\t")
    # print("reached3.2")
    topic = tokens.pop()
    # print("reached3.3")
    message = '\t'.join(tokens)
    # print(tokens)
    # print(topic) 
    # print(message)
    while not producer.enqueue(topic, message):
        # failure... 
        # keep on quering producer size
        print("reached3.5")
        time.sleep(random.uniform(0, max_timeout))
        # print("reached3.6")
        # print(tokens + " " + topic)

    file.write(producer.name + ' enqueued ' + message + ' at ' + topic + '\n')
    # print("reached3.7")
    time.sleep(random.uniform(0, max_timeout))
    print("reached3.8")

print(producer.name, 'finished producing!')
    