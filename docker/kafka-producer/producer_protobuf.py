import sys
import time
import random
import message_pb2
from kafka import KafkaProducer


class PseudoRandom(object):
    "Generate pseudo-random string with same seed"

    def __init__(self, seed=int(time.time()), chars='123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'):
        self.stage = seed
        self.chars = chars

    def choice(self, chars):
        self.stage = (8121 * self.stage + 28411) % 134456
        index = self.stage % len(self.chars)
        return self.chars[index]

    def RandString(self, length=12):
        return ''.join(self.choice(self.chars) for _ in range(length))


def main():
    if len(sys.argv) > 1:
        bootstrap_servers = sys.argv[1]
    else:
        bootstrap_servers = "kafka:9092"
    topic_name = "stream_protobuf"

    # wait for server started
    time.sleep(3)
    rand = PseudoRandom(1314)

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: v.SerializeToString())
    print("Generate json data to {}, bootstrap_servers={}".format(topic_name, bootstrap_servers))

    while True:
        try:
            # Gen keys and shuffle send
            array = []
            for i in range(3):
                key = rand.RandString()
                msg = message_pb2.Message()
                msg.key = key
                msg.value = "val#%s" % key[6:]
                msg.ms_time = int(time.time() * 1000)
                array.append(msg)
            random.shuffle(array)

            for msg in array:
                future = producer.send(topic_name, msg)
                result = future.get(timeout=10)
                print(result)
            time.sleep(1.0)

        except Exception as e:
            print(e)
            time.sleep(3.0)


if __name__ == "__main__":
    main()
