import logging
import json
import argparse
import os
import sys
import signal
from collections import defaultdict
from urllib.parse import urlparse
from datetime import datetime
from confluent_kafka import DeserializingConsumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

broker = 'broker:9092'
domain_counts = defaultdict(int)
processed_orders = set()


class MessageObj(object):
    def __init__(self, order=None, url=None):
        self.order = order
        self.url = url


class MessageObjDecoder:
    def __call__(self, value, ctx):
        data = json.loads(value.decode("utf-8"))
        return MessageObj(order=int(data['order']),
                          url=data['url'])


def signal_handler(sig, frame):
    print("Received SIGTERM, shutting down gracefully...")
    sys.exit(0)


def process_messages(topic):

    signal.signal(signal.SIGTERM, signal_handler)
    config = {
        'bootstrap.servers': 'broker:9092',
        'group.id': 'primary',
        'auto.offset.reset': 'earliest'
    }
    config['key.deserializer'] = StringDeserializer()
    config['value.deserializer'] = MessageObjDecoder()
    c = DeserializingConsumer(config)
    c.subscribe([topic])

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            message = msg.value()
            log_top_domains(message)

    except KafkaException as e:
        print(f"Kafka error: {e}")
    except KeyboardInterrupt:
        print("Aborted by user")
    finally:
        c.close()


def log_top_domains(message):
    order = message.order if not isinstance(
        message.order, tuple) else message.order[0]

    if order in processed_orders:
        return
    processed_orders.add(order)

    url = message.url if not isinstance(message.url, tuple) else message.url[0]
    parsed_url = urlparse(url)
    netloc = parsed_url.netloc
    if ':' in netloc:
        return
    root_domain = parsed_url.netloc.split('.')[-1]
    domain_counts[root_domain] += 1

    if sum(domain_counts.values()) % 100 == 0:  # print top 5 domains after every 100 messages
        top_domains = sorted(domain_counts.items(),
                             key=lambda item: item[1], reverse=True)[:5]
        logger.info("Top 5 root domains so far:")
        for domain, count in top_domains:
            logger.info(f"{domain}: {count}")


def delivery_callback(err, msg):
    if err is not None:
        print("Delivery failed to {} topic at {} for {} with {} offset: {}".format(
            msg.topic(), msg.partition(), msg.key(), msg.offset(), err))
        return


def parse_args():
    parser = argparse.ArgumentParser(
        description='Consume messages from Kafka and log details')
    parser.add_argument('--topic', required=False, default='history',
                        help='Kafka topic to consume messages from')
    return parser.parse_args()


def main():
    args = parse_args()
    topic = os.getenv('TOPIC', args.topic)
    process_messages(topic)


if __name__ == '__main__':
    main()
