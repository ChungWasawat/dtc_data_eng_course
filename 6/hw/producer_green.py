import csv
from json import dumps
from kafka import KafkaProducer
from time import sleep
from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH_G, KAFKA_TOPIC_G

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

file = open(INPUT_DATA_PATH_G)

csvreader = csv.reader(file)
header = next(csvreader)
for row in csvreader:
    key = {"pu_location_id": int(row[5])}
    value = {"vendor_id": str(row[0]), "pu_location_id": int(row[5]), "do_location_id": int(row[6])}
    producer.send(KAFKA_TOPIC_G, value=value, key=key)
    print(f"producing data {int(row[5])}")
    sleep(1)