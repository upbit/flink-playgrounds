import sys
import json
import time
from kafka import KafkaProducer


class PseudoRandom(object):
    "Generate pseudo-random string with same seed"

    def __init__(self, seed=int(time.time()), chars='0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'):
        self.stage = seed
        self.chars = chars

    def choice(self, stage, update=True):
        index = (stage * 33) % len(self.chars)
        if update:
            self.stage = (stage + index) % 2220301
        return self.chars[index]

    def RandString(self, length=12):
        return ''.join(self.choice(self.stage) for _ in range(length))


def main():
    if len(sys.argv) > 1:
        bootstrap_servers = sys.argv[1]
    else:
        bootstrap_servers = "kafka:9092"
    topic_name = "stream_json"

    # wait for server started
    time.sleep(3.0)
    rand = PseudoRandom(1314)

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    print("Generate json data to {}, bootstrap_servers={}".format(topic_name, bootstrap_servers))

    while True:
        try:
            key = rand.RandString()
            value = {"key": key, "value": "val[%s]" % key[:6], "timestamp": int(time.time())}
            future = producer.send(topic_name, value)
            result = future.get(timeout=10)
            print(result)
            time.sleep(1.0)

        except Exception as e:
            print(e)
            time.sleep(3.0)


if __name__ == "__main__":
    main()
