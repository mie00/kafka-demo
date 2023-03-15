import random

import requests
from confluent_kafka import Consumer, KafkaError, KafkaException

topic = "mohamed-elawadi-7"
group_id = "mohamed.elawad-13"

conf = {'bootstrap.servers': "34.70.120.136:9094,35.202.98.23:9094,34.133.105.230:9094",
        'group.id': group_id,
        'enable.auto.commit': False,
        'auto.offset.reset': 'smallest'}


def detect_object(id):
    return random.choice(['car', 'house', 'person'])

consumer = Consumer(conf)
try:
    consumer.subscribe([topic])
    print('consuming')

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                    (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            print("message received", msg.value())
            requests.put('http://127.0.0.1:5000/object/' + msg.value().decode(), json={"object": detect_object(msg.value().decode())})
            consumer.commit(asynchronous=False)
finally:
    consumer.close()
