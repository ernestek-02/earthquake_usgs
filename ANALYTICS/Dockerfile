FROM apache/spark:4.0.0

USER root

WORKDIR /app

COPY analytics.py .

CMD ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "--packages", "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0", "/app/analytics.py"]