import csv
from json import dumps
from kafka import KafkaProducer
from time import sleep
from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH_F, KAFKA_TOPIC_F

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

file = open(INPUT_DATA_PATH_F)

csvreader = csv.reader(file)
header = next(csvreader)
for row in csvreader:
    key = {"vendorId": int(row[0])}
    value = {"vendorId": int(row[0]), "passenger_count": int(row[3]), "trip_distance": float(row[4]), "payment_type": int(row[9]), "total_amount": float(row[16])}
    producer.send(KAFKA_TOPIC_F, value=value, key=key)
    print(f"producing data {key}")
    sleep(1)