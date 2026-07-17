FROM apache/spark:python3

USER root

# Postgres JDBC driver — needed for the JDBC sink in fraud_detector.py
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    /opt/spark/jars/postgresql-42.7.3.jar

# Copy application code
WORKDIR /app
COPY . .

# -----------------------------------------------------------------------
# spark-submit packages fetched from Maven Central at first startup:
#   spark-sql-kafka  — Kafka source connector
#   hadoop-aws       — s3a:// filesystem for MinIO / S3
#   aws-java-sdk     — required by hadoop-aws
#
# These JARs are cached by Docker on subsequent builds (the --packages
# flag downloads them into /root/.ivy2 which is baked into the layer).
# -----------------------------------------------------------------------
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", \
     "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.hadoop:hadoop-aws:3.5.0,com.amazonaws:aws-java-sdk-bundle:1.12.780", \
     "--conf", "spark.sql.shuffle.partitions=4", \
     "fraud_detector.py"]
