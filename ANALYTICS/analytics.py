import os

from pymongo import MongoClient

MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = int(os.getenv("MONGO_PORT"))
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]
total = collection.count_documents({})

print(f"Numero totale terremoti registrati: {total}")

pipeline = [
    {
        "$group": {
            "_id": None,
            "magnitudo_media": {"$avg": "$magnitude"},
            "magnitudo_massima": {"$max": "$magnitude"},
            "profondita_media": {"$avg": "$depth"}
        }
    }
]

result = list(collection.aggregate(pipeline))

if result:
    stats = result[0]

    print(f"Magnitudo media: {stats['magnitudo_media']:.1f}")
    print(f"Magnitudo massima: {stats['magnitudo_massima']:.1f}")
    print(f"Profondità media: {stats['profondita_media']:.2f} km")


print("\nTop 3 terremoti più forti:")

earthquakes = collection.find().sort("magnitude", -1).limit(3)

for earthquake in earthquakes:
    print(
        earthquake["place"],
        "- Magnitudo:",
        earthquake["magnitude"]
    )