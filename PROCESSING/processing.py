import json
import os

from kafka import KafkaConsumer
from pymongo import MongoClient


KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = int(os.getenv("MONGO_PORT"))
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")


consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest"
)

mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)

db = mongo_client[MONGO_DB]
collection = db[MONGO_COLLECTION]

collection.create_index("id", unique=True)

print("Processing avviato...", flush=True)

for message in consumer:

    earthquake = json.loads(message.value.decode("utf-8"))

    properties = earthquake["properties"]
    coordinates = earthquake["geometry"]["coordinates"]

    document = {
        "id": earthquake["id"],
        "magnitude": properties["mag"],
        "place": properties["place"],
        "time": properties["time"],
        "longitude": coordinates[0],
        "latitude": coordinates[1],
        "depth": coordinates[2]
    }

    collection.update_one(
        {"id": document["id"]},
        {"$set": document},
        upsert=True
    )

    print(
        f"Salvato terremoto: {document['id']}",
        flush=True
    )