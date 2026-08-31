import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max


MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = os.getenv("MONGO_PORT")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")


mongo_uri = (
    f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"
    f"{MONGO_DB}.{MONGO_COLLECTION}"
)


spark = (
    SparkSession.builder
    .appName("EarthquakeAnalytics")
    .config(
        "spark.mongodb.read.connection.uri",
        mongo_uri
    )
    .getOrCreate()
)


print("Spark Analytics avviato...", flush=True)


while True:

    df = (
        spark.read
        .format("mongodb")
        .load()
    )

    total = df.count()

    if total > 0:

        stats = (
            df.agg(
                avg("magnitude").alias("magnitudo_media"),
                max("magnitude").alias("magnitudo_massima"),
                avg("depth").alias("profondita_media")
            )
            .collect()[0]
        )

        result = spark.createDataFrame(
            [
                (
                    "global",
                    total,
                    stats["magnitudo_media"],
                    stats["magnitudo_massima"],
                    stats["profondita_media"]
                )
            ],
            [
                "id",
                "total",
                "magnitudo_media",
                "magnitudo_massima",
                "profondita_media"
            ]
        )

        (
            result.write
            .format("mongodb")
            .mode("overwrite")
            .option(
                "spark.mongodb.write.connection.uri",
                f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"
                f"{MONGO_DB}.analytics_results"
            )
            .save()
        )

        print(
            f"Analytics aggiornate - "
            f"Totale: {total}",
            flush=True
        )

    time.sleep(5)