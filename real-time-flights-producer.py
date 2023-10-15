import json
import time
import requests

from kafka import KafkaProducer

FLIGHT_KAFKA_TOPIC = "Flight-Info"

producer = KafkaProducer(
    bootstrap_servers="localhost:29092")


# Calling AirLab API
API_KEY = "dc019782-c207-4c85-85d3-bb4d4e1d0845"
url = f"https://airlabs.co/api/v9/flights?api_key={API_KEY}"

payload = {}
headers = {}

response = requests.request(
    "GET"
    , url
    , headers=headers
    , data=payload)

data = response.json()
datalist = data['response']

# Loop Over data and send to topic
for i in range(0, len(data['response'])):
    
    producer.send(
        FLIGHT_KAFKA_TOPIC
        , json.dumps(datalist[i]).encode("utf-8")
    )
    
    print(f"Done Sending..{i}")
    time.sleep(0.2)