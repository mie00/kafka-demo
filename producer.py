import sys
import uuid

from confluent_kafka import Producer

topic = "mohamed-elawadi-7"
client_id = "mohamed.elawad-1"

conf = {'bootstrap.servers': "34.70.120.136:9094,35.202.98.23:9094,34.133.105.230:9094",
        'client.id': client_id}



producer = Producer(conf)

my_id = uuid.uuid4()
producer.produce(topic, key=my_id.hex, value=sys.argv[1])
producer.flush()
