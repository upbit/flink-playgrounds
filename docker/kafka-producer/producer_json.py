import sys
import json
import time
import random
from kafka import KafkaProducer


if len(sys.argv) > 1:
    bootstrap_servers = sys.argv[1]
else:
    bootstrap_servers = "kafka:9092"
topic_name = "stream_json"


# wait for server started
time.sleep(3)

producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
print("Generate json data to {}, bootstrap_servers={}".format(topic_name, bootstrap_servers))

keys = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
while True:
    future = producer.send(topic_name, {"key": random.choice(keys), "value": random.random()})
    result = future.get(timeout=10)
    print(result)
    time.sleep(0.1 + 1.0 * random.random())
