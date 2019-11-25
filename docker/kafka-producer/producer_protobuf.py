import sys
import time
import random
import message_pb2
from kafka import KafkaProducer


if len(sys.argv) > 1:
    bootstrap_servers = sys.argv[1]
else:
    bootstrap_servers = "kafka:9092"
topic_name = "stream_protobuf"


# wait for server started
time.sleep(3)

producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: v.SerializeToString())
print("Generate json data to {}, bootstrap_servers={}".format(topic_name, bootstrap_servers))

keys = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
keyMap = {
    "Sun": "Sunday",
    "Mon": "Monday",
    "Tue": "Tuesday",
    "Wed": "Wednesday",
    "Thu": "Thursday",
    "Fri": "Friday",
    "Sat": "Saturday",
}
while True:
    msg = message_pb2.Message()
    msg.key = random.choice(keys)
    msg.value = keyMap[msg.key] + "#%d" % random.randint(0, 1000)

    future = producer.send(topic_name, msg)
    result = future.get(timeout=10)
    print(result)
    time.sleep(0.1 + 1.0 * random.random())
