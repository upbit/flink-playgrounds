#!/usr/bin/python
#-*- coding:utf-8 -*-

import json
import argparse
from kafka import KafkaConsumer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("topic", type=str,
                        help="Topic for producer")
    parser.add_argument("--bootstrap_servers", type=str, default="kafka:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--group_id", type=str, default="simple_consumer",
                        help="Group ID for kafka consumer")
    args = parser.parse_args()

    print(">> Consumer data from {}, bootstrap_servers={}".format(args.topic, args.bootstrap_servers))

    consumer = KafkaConsumer(args.topic, group_id=args.group_id, bootstrap_servers=args.bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    for msg in consumer:
        print(msg)


if __name__ == "__main__":
    main()
