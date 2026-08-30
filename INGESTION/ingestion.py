import json
import os
import time

import requests
from kafka import KafkaProducer


USGS_URL = os.getenv("USGS_URL")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL"))

print("Avvio ingestion...", flush=True)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER
)


while True:

    response = requests.get(USGS_URL)
    data = response.json()

    earthquakes = data["features"]

    for earthquake in earthquakes:
        producer.send(
            KAFKA_TOPIC,
            json.dumps(earthquake).encode("utf-8")
        )

    producer.flush()

    print(f"Inviati {len(earthquakes)} terremoti a Kafka", flush=True)

    time.sleep(POLL_INTERVAL)