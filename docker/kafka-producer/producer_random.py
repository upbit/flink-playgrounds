#!/usr/bin/python
#-*- coding:utf-8 -*-

import sys
import json
import time
import random
import argparse
from kafka import KafkaProducer


class PseudoRandom(object):
    "Generate pseudo-random string with same seed"

    def __init__(self, seed=int(time.time()), chars='123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'):
        self.stage = seed
        self.chars = chars
        self.stack = []

    def choice(self, chars):
        self.stage = (8121 * self.stage + 28411) % 134456
        index = self.stage % len(self.chars)
        return self.chars[index]

    def RandString(self, length=12):
        "输出指定长度的随机序列"
        return ''.join(self.choice(self.chars) for _ in range(length))

    def RandStackString(self, size=10, length=12):
        "以size大小的随机序输出序列"
        while len(self.stack) < size:
            self.stack.append(self.RandString(length))
        random.shuffle(self.stack)
        return self.stack.pop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("topic", type=str,
                        help="Topic for producer")
    parser.add_argument("--bootstrap_servers", type=str, default="kafka:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--seed", type=int, default=1314,
                        help="Seed for pseudo-random generater")
    parser.add_argument("--size", type=int, default=100,
                        help="Size of random pick for pseudo-random generater")
    parser.add_argument("--skip_ratio", type=float, default=0.0,
                        help="Skip random data input if random below the ratio")
    parser.add_argument("--random_int", action='store_true',
                        help="Add random int to message")
    parser.add_argument("--random_float", action='store_true',
                        help="Add random float to message")
    args = parser.parse_args()

    # wait for kafka
    time.sleep(5.0)

    rand = PseudoRandom(args.seed)
    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    print("Generate random data to {}, bootstrap_servers={}".format(args.topic, args.bootstrap_servers))

    while True:
        try:
            key = rand.RandStackString(size=args.size)
            # 小于 skip_ratio 则重新生成
            if random.random() < args.skip_ratio:
                time.sleep(0.1)
                continue

            data = {
                "key": key,
                "value": "Value: " + key[:6] + "-" + key[6:] + "1".zfill(300),
                "timestamp": int(time.time()),
            }
            if args.random_int:
                data['int'] = random.randint(0, 100)
            if args.random_float:
                data['float'] = 3 * random.random()

            future = producer.send(args.topic, data)
            result = future.get(timeout=10)
            print(result)
            time.sleep(0.1)

        except Exception as e:
            print("[ERROR] %s" % e)
            time.sleep(1.0)

if __name__ == "__main__":
    main()
