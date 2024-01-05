import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime

def create_response_dict():

    api_key = "YOUR API KEY"
    url = f"https://api.nasa.gov/planetary/apod?api_key={api_key}"
    response = requests.get(url)
    data = response.json()
    return data

def create_final_json(data):

    return [{
        "title": data["title"],
        "explanation": data["explanation"],
        "url": data["url"],
        "hdurl": data.get("hdurl"),
        "date": data["date"]
    }]

def create_kafka_producer():

    return KafkaProducer(bootstrap_servers=['kafka1:19092', 'kafka2:19093', 'kafka3:19094'])

def start_streaming():

    producer = create_kafka_producer()
    data = create_response_dict()
    kafka_messages = create_final_json(data)    

    for message in kafka_messages:
        producer.send("nasa_apod", json.dumps(message).encode('utf-8'))
        time.sleep(1)

if __name__ == "__main__":
    start_streaming()

