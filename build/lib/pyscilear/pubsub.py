import json

from kafka import KafkaConsumer, KafkaProducer


def get_publisher():
    servers = '192.168.0.10:9092'
    producer = KafkaProducer(bootstrap_servers=servers)
    return producer


def pub(data, topic):
    get_publisher().send(topic, json.dumps(data, default=str).encode())


def get_subscriber(group_id, auto_offset_reset):
    servers = '192.168.0.10:9092'
    consumer = KafkaConsumer(bootstrap_servers=servers,
                             auto_offset_reset=auto_offset_reset,
                             group_id=group_id)
    return consumer


def get_topic_consumer(topic, consumer_group_id=None, auto_offset_reset='earliest'):
    consumer = get_subscriber(consumer_group_id, auto_offset_reset)
    consumer.subscribe([topic])
    return consumer


def decode_message(message):
    return json.loads(message.value.decode('utf-8'))
