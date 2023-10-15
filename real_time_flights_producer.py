import json
import time
import requests

from kafka import KafkaProducer

def create_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers="localhost:29092")
    return producer

def get_Airlabs_data():
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
    return data

def start_streaming():
    FLIGHT_KAFKA_TOPIC = "Flight-Info"
    producer = create_kafka_producer()
    data = get_Airlabs_data()

    # Loop Over data and send to topic
    for idx, x in enumerate(data['response']):
        
        producer.send(
            FLIGHT_KAFKA_TOPIC
            , json.dumps(x).encode("utf-8")
        )
        
        print(f"Done Sending..{idx}")
        