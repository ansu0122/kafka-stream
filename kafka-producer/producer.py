import os
import sys
import signal
import argparse
import json
import csv
from datetime import datetime
from confluent_kafka import Producer, KafkaException
from confluent_kafka.serialization import StringSerializer
from adminapi import create_topic

broker = 'broker:9092'


class MessageObj(object):
    def __init__(self, order=None, url=None):
        self.order = order
        self.url = url


class MessageObjEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, MessageObj):
            return {
                'order': obj.order,
                'url': obj.url
            }
        else:
            return super().default(obj)


def signal_handler(sig, frame):
    print("Received SIGTERM, shutting down gracefully...")
    sys.exit(0)


def produce_messages(topic, history_file):

    signal.signal(signal.SIGTERM, signal_handler)

    p = Producer({'bootstrap.servers': broker})

    string_serializer = StringSerializer('utf_8')
    try:
        with open(history_file, 'r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            if 'order' not in reader.fieldnames or 'url' not in reader.fieldnames:
                raise ValueError("Browser history file must contain 'order' and 'url' columns")
            
            for num, row in enumerate(reader):
                msg = MessageObj(order=row['order'], url=row['url'])
                p.produce(topic, key=string_serializer(str(num)),
                          value=json.dumps(
                              msg, cls=MessageObjEncoder).encode("utf-8"),
                          callback=delivery_callback)

                p.poll(0)
    except KafkaException as e:
        print(f"Kafka error: {e}")
    except KeyboardInterrupt:
        print("Aborted by user")
    finally:
        p.flush()


def delivery_callback(err, msg):
    if err is not None:
        print("Delivery failed to {} topic {}: {}".format(
            msg.topic(), msg.key(), err))
        return
    print('Message {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def parse_args():
    parser = argparse.ArgumentParser(
        description='Stream browser history to Kafka')
    parser.add_argument('--topic', required=False, default='history',
                        help='Kafka topic to publish messages to')
    parser.add_argument('--data-dir', required=False, default='data/',
                        help='Directory with csv file(s)')
    return parser.parse_args()


def find_csv_file(data_dir):
    for root, _, files in os.walk(data_dir):
        for file in files:
            if file.endswith(('.csv')):
                return os.path.join(root, file)
    return None


def main():
    args = parse_args()
    topic = os.getenv('TOPIC', args.topic)
    data_dir = os.getenv('DATA_DIR', args.data_dir)

    video_file = find_csv_file(data_dir)
    if not video_file:
        raise Exception(
            f"Please provide a csv file in the specified directory: {data_dir}")

    create_topic(broker, topic)

    produce_messages(topic, video_file)


if __name__ == '__main__':
    main()
