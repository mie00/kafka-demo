import random
import sqlite3

import requests
from confluent_kafka import Consumer, KafkaError, KafkaException
from PIL import Image

topic = "mohamed-elawadi-7"
group_id = "mohamed.elawad-grayscale-13"

conf = {'bootstrap.servers': "34.70.120.136:9094,35.202.98.23:9094,34.133.105.230:9094",
        'group.id': group_id,
        'enable.auto.commit': False,
        'auto.offset.reset': 'smallest'}

MAIN_DB = "main.db"

def get_db_connection():
    conn = sqlite3.connect(MAIN_DB)
    conn.row_factory = sqlite3.Row
    return conn

def get_image_path(id):
    con = get_db_connection()
    cur = con.cursor()
    res = cur.execute("SELECT filename FROM image WHERE id = ?", (id,))
    row = res.fetchone()
    con.close()
    return 'images/'+row['filename']

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
            try:
                print("message received", msg.value())
                image_path = get_image_path(msg.value().decode())
                img = Image.open(image_path).convert('L')
                img.save(image_path)
                consumer.commit(asynchronous=False)
            except:
                pass
finally:
    consumer.close()
