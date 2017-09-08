import json

from kafka import KafkaConsumer, KafkaProducer

PRODUCERS = dict()
SUBSCRIBERS = dict()


def get_publisher():
    global PRODUCERS
    servers = '192.168.0.10:9092'
    if servers not in PRODUCERS:
        producer = KafkaProducer(bootstrap_servers=servers)
        PRODUCERS[servers] = producer
    else:
        producer = PRODUCERS[servers]
    return producer


def pub(data, topic, auto_flush=True):
    pub = get_publisher()
    pub.send(topic=topic, value=json.dumps(data, default=str).encode())
    if auto_flush:
        get_publisher().flush()


def get_subscriber(group_id, auto_offset_reset):
    global SUBSCRIBERS
    servers = '192.168.0.10:9092'
    data = (servers, group_id)
    if data not in SUBSCRIBERS:
        consumer = KafkaConsumer(bootstrap_servers=servers,
                                 auto_offset_reset=auto_offset_reset,
                                 group_id=group_id)
        SUBSCRIBERS[data] = consumer
    else:
        consumer = SUBSCRIBERS[data]
    return consumer


def get_topic_consumer(topic, consumer_group_id=None, auto_offset_reset='earliest'):
    consumer = get_subscriber(consumer_group_id, auto_offset_reset)
    consumer.subscribe([topic])
    return consumer


def decode_message(message):
    return json.loads(message.value.decode('utf-8'))
