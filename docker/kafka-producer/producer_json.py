import sys
import json
import time
from kafka import KafkaProducer


if len(sys.argv) >= 1:
    bootstrap_servers = sys.argv[1]
else:
    bootstrap_servers = "kafka:9092"
topic_name = "stream_json"


# wait for server started
time.sleep(3)

producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

sys.stdout.write("Generate json data to {}, bootstrap_servers={}\n".format(topic_name, bootstrap_servers))

while True:
    producer.send(topic_name, {"Hello": "World", "foo": 1, "bar": 2.0})
    producer.flush()
    time.sleep(1)
    sys.stdout.write(".")
    sys.stdout.flush()
