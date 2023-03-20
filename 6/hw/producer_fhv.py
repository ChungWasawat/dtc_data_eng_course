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
    # there are some rows with empty pu_location_id
    if row[3] != "":
        key = {"pu_location_id": int(row[3])}
        value = {"dispatching_base_num": str(row[0]), "pu_location_id": int(row[3]), "do_location_id": int(row[4])}
        producer.send(KAFKA_TOPIC_F, value=value, key=key)
        print(f"producing data {int(row[3])}")
        sleep(1)
    