import json

from kafka import KafkaConsumer
from kafka import KafkaProducer

from pymongo import MongoClient

FLIGHT_KAFKA_TOPIC = "Flight-Info"

consumer = KafkaConsumer(
    FLIGHT_KAFKA_TOPIC, 
    bootstrap_servers="localhost:29092"
)

# Connect to MDB
username = 'admin'
password = 'admin'
url = f"mongodb+srv://{username}:{password}@cluster0.5shoywa.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(url)
col = client['kafka_data']['flight_data']

while True:
    for message in consumer:
        print("Ongoing transaction..")
        
        consumed_message = json.loads(message.value.decode())
        # print(consumed_message)

        # Create dictionary and ingest data into MongoDB
        try:
            col.insert_one(consumed_message)
            print(f"Data inserted with record ids", consumed_message)
        except:
            print("Could not insert into MongoDB")