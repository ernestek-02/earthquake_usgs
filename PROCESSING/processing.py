import json
import os

from kafka import KafkaConsumer
from pymongo import MongoClient


KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

MONGO_HOSTS = os.getenv("MONGO_HOSTS")
MONGO_REPLICA_SET = os.getenv("MONGO_REPLICA_SET")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

if not all([
    MONGO_HOSTS,
    MONGO_REPLICA_SET,
    MONGO_DB,
    MONGO_COLLECTION
]):
    raise RuntimeError(
        "Configurazione MongoDB incompleta: "
        "controllare MONGO_HOSTS, MONGO_REPLICA_SET, "
        "MONGO_DB e MONGO_COLLECTION"
    )

MONGO_URI = (
    f"mongodb://{MONGO_HOSTS}/"
    f"?replicaSet={MONGO_REPLICA_SET}"
)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    group_id="earthquake-processing"
)

mongo_client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=10000
)

mongo_client.admin.command("ping")

db = mongo_client[MONGO_DB]
collection = db[MONGO_COLLECTION]

collection.create_index("id", unique=True)
collection.create_index("time")
collection.create_index("magnitude")

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