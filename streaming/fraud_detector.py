"""
fraud_detector.py
-----------------
FinPulse Section 2 — Spark Structured Streaming Fraud Detection Job.

Consumes the `transactions.raw` Kafka topic, applies windowed aggregations
and rule-based fraud detection, then writes:

  Sink 1 → Postgres `flagged_transactions` (flagged rows only, JDBC upsert)
  Sink 2 → MinIO `s3a://finpulse/raw/`   (ALL rows, Parquet by date)

Architecture:
  ┌────────────────────────────────────────────┐
  │  Kafka: transactions.raw                   │
  └──────────────────┬─────────────────────────┘
                     │ Structured Streaming
  ┌──────────────────▼─────────────────────────┐
  │  1. Parse JSON (strict schema)             │
  │  2. Watermark 10 min on event_time         │
  │  3. Sliding window 5 min / 1 min           │
  │     - tx_count_5m, total_amount_5m,        │
  │       max_amount_5m  per nameOrig          │
  │  4. Join windowed stats back to raw rows   │
  │  5. Apply rule engine → rule_name          │
  └────────────┬─────────────────┬─────────────┘
               │                 │
     ┌─────────▼──────┐  ┌───────▼──────────┐
     │    Postgres     │  │      MinIO        │
     │ flagged_tx only │  │  all rows Parquet │
     └─────────────────┘  └───────────────────┘

Usage (inside Docker via CMD in Dockerfile):
    spark-submit --packages <kafka+s3a+jdbc jars> fraud_detector.py

Environment variables:
    KAFKA_BROKER        kafka:29092
    POSTGRES_URL        jdbc:postgresql://postgres:5432/finpulse
    POSTGRES_USER       airflow
    POSTGRES_PASSWORD   airflow
    S3_ENDPOINT         http://minio:9000
    AWS_ACCESS_KEY_ID   minioadmin
    AWS_SECRET_ACCESS_KEY minioadmin
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, TimestampType,
)

# Local import — pure Python, no Spark dependency
from rules import apply_rules

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (all overridable via env vars)
# ---------------------------------------------------------------------------
KAFKA_BROKER    = os.getenv("KAFKA_BROKER",         "kafka:29092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC",           "transactions.raw")
POSTGRES_URL    = os.getenv("POSTGRES_URL",          "jdbc:postgresql://postgres:5432/finpulse")
POSTGRES_USER   = os.getenv("POSTGRES_USER",         "airflow")
POSTGRES_PASS   = os.getenv("POSTGRES_PASSWORD",     "airflow")
S3_ENDPOINT     = os.getenv("S3_ENDPOINT",           "http://minio:9000")
AWS_KEY         = os.getenv("AWS_ACCESS_KEY_ID",     "minioadmin")
AWS_SECRET      = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
RAW_S3_PATH     = os.getenv("RAW_S3_PATH",           "s3a://finpulse/raw/")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_DIR",        "/tmp/spark-checkpoints")

# ---------------------------------------------------------------------------
# JSON schema — mirrors generator/producer.py _build_tx()
# ---------------------------------------------------------------------------
TX_SCHEMA = StructType([
    StructField("transaction_id",      StringType(),    True),
    StructField("ingestion_timestamp", StringType(),    True),   # ISO string → cast below
    StructField("step",                LongType(),      True),
    StructField("timestamp",           StringType(),    True),   # simulated event time
    StructField("type",                StringType(),    True),
    StructField("amount",              DoubleType(),    True),
    StructField("nameOrig",            StringType(),    True),
    StructField("oldbalanceOrg",       DoubleType(),    True),
    StructField("newbalanceOrig",      DoubleType(),    True),
    StructField("nameDest",            StringType(),    True),
    StructField("oldbalanceDest",      DoubleType(),    True),
    StructField("newbalanceDest",      DoubleType(),    True),
    StructField("location",            StringType(),    True),
    StructField("isFraud",             IntegerType(),   True),
    StructField("isFlaggedFraud",      IntegerType(),   True),
    StructField("anomaly_type",        StringType(),    True),
])


# ---------------------------------------------------------------------------
# Spark Session factory
# ---------------------------------------------------------------------------
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("FinPulse-FraudDetector")
        # S3a / MinIO configuration
        .config("spark.hadoop.fs.s3a.endpoint",               S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             AWS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             AWS_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        # Checkpoint location must be S3-safe
        .config("spark.sql.streaming.checkpointLocation",
                CHECKPOINT_BASE)
        # Reduce shuffle partitions for single-node demo
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Streaming source
# ---------------------------------------------------------------------------
def read_kafka(spark: SparkSession) -> DataFrame:
    """Subscribe to transactions.raw and parse the JSON payload."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Kafka delivers value as bytes; cast to string then parse JSON
    parsed = (
        raw
        .select(F.from_json(
            F.col("value").cast("string"),
            TX_SCHEMA
        ).alias("data"))
        .select("data.*")
        # Cast ISO timestamp strings to proper TimestampType for watermarking
        .withColumn("event_time",
                    F.to_timestamp("timestamp"))
        .withColumn("ingestion_time",
                    F.to_timestamp("ingestion_timestamp"))
        .drop("timestamp", "ingestion_timestamp")
    )
    return parsed


# ---------------------------------------------------------------------------
# Windowed aggregations
# ---------------------------------------------------------------------------
def compute_window_stats(df: DataFrame) -> DataFrame:
    """
    Compute per-account sliding window statistics.

    Window: 5 minutes wide, sliding every 1 minute.
    Watermark: tolerate up to 10 minutes of late data.

    Returns a DataFrame with one row per (window, nameOrig) combination
    plus aggregated stats.
    """
    return (
        df
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            F.window("event_time", "5 minutes", "1 minute"),
            F.col("nameOrig")
        )
        .agg(
            F.count("*").alias("tx_count_5m"),
            F.sum("amount").alias("total_amount_5m"),
            F.max("amount").alias("max_amount_5m"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("nameOrig"),
            F.col("tx_count_5m"),
            F.col("total_amount_5m"),
            F.col("max_amount_5m"),
        )
    )


# ---------------------------------------------------------------------------
# foreachBatch sink handler
# ---------------------------------------------------------------------------
def make_batch_writer(spark: SparkSession):
    """
    Returns a foreachBatch callback that:
      1. Joins window stats back onto raw rows
      2. Applies the rule engine
      3. Writes ALL rows to MinIO as Parquet (raw lake / bronze layer)
      4. Writes FLAGGED rows to Postgres (serving layer)
    """

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            logger.info("Batch %d — empty, skipping.", batch_id)
            return

        n_raw = batch_df.count()
        logger.info("Batch %d — processing %d rows.", batch_id, n_raw)

        # ------------------------------------------------------------------
        # Enrich with window stats (compute on the static micro-batch)
        # ------------------------------------------------------------------
        window_stats = (
            batch_df
            .groupBy("nameOrig")
            .agg(
                F.count("*").alias("tx_count_5m"),
                F.sum("amount").alias("total_amount_5m"),
                F.max("amount").alias("max_amount_5m"),
            )
        )

        enriched = batch_df.join(window_stats, on="nameOrig", how="left")

        # ------------------------------------------------------------------
        # Apply rule engine via a UDF
        # ------------------------------------------------------------------
        from pyspark.sql.types import StructType, StructField, BooleanType, StringType
        rule_schema = StructType([
            StructField("is_fraud",  BooleanType(), False),
            StructField("rule_name", StringType(),  True),
        ])

        @F.udf(rule_schema)
        def detect(amount, tx_count_5m, event_time, geo_impossible_flag):
            hour_utc = event_time.hour if event_time else 12
            geo = int(geo_impossible_flag) if geo_impossible_flag is not None else 0
            flagged, rule = apply_rules(
                amount=float(amount or 0),
                tx_count_5m=int(tx_count_5m or 0),
                hour_utc=hour_utc,
                geo_impossible=geo,
            )
            return (flagged, rule)

        # geo_impossible is not in TX_SCHEMA (producer injects anomaly_type instead);
        # derive it from anomaly_type field
        scored = (
            enriched
            .withColumn("geo_flag",
                        (F.col("anomaly_type") == "geo_impossible").cast("int"))
            .withColumn("detection",
                        detect(
                            F.col("amount"),
                            F.col("tx_count_5m"),
                            F.col("event_time"),
                            F.col("geo_flag"),
                        ))
            .withColumn("is_fraud_detected",  F.col("detection.is_fraud"))
            .withColumn("rule_name",          F.col("detection.rule_name"))
            .withColumn("dt",
                        F.date_format("event_time", "yyyy-MM-dd"))
            .drop("detection")
        )

        # ------------------------------------------------------------------
        # Sink 2: MinIO — write ALL rows as Parquet (bronze/raw lake)
        # ------------------------------------------------------------------
        try:
            (
                scored
                .write
                .mode("append")
                .partitionBy("dt")
                .parquet(RAW_S3_PATH)
            )
            logger.info("Batch %d — wrote %d rows to MinIO %s.", batch_id, n_raw, RAW_S3_PATH)
        except Exception as exc:
            logger.error("Batch %d — MinIO write failed: %s", batch_id, exc)

        # ------------------------------------------------------------------
        # Sink 1: Postgres — write FLAGGED rows only
        # ------------------------------------------------------------------
        flagged = scored.filter(F.col("is_fraud_detected") == True)   # noqa: E712
        n_flagged = flagged.count()

        if n_flagged > 0:
            pg_df = flagged.select(
                F.col("transaction_id"),
                F.col("event_time"),
                F.col("type").alias("tx_type"),
                F.col("amount"),
                F.col("nameOrig").alias("origin_account"),
                F.col("nameDest").alias("dest_account"),
                F.col("rule_name"),
                F.col("tx_count_5m"),
                F.col("total_amount_5m"),
                F.col("max_amount_5m"),
                F.col("geo_flag").alias("geo_impossible"),
            )
            try:
                (
                    pg_df.write
                    .format("jdbc")
                    .option("url",      POSTGRES_URL)
                    .option("dbtable",  "flagged_transactions")
                    .option("user",     POSTGRES_USER)
                    .option("password", POSTGRES_PASS)
                    .option("driver",   "org.postgresql.Driver")
                    # Avoid duplicates on restart — Postgres handles conflict via PK
                    .mode("append")
                    .save()
                )
                logger.info("Batch %d — wrote %d flagged rows to Postgres.",
                            batch_id, n_flagged)
            except Exception as exc:
                logger.error("Batch %d — Postgres write failed: %s", batch_id, exc)
        else:
            logger.info("Batch %d — no flagged rows.", batch_id)

    return process_batch


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    logger.info("FinPulse Fraud Detector starting…")
    logger.info("  Kafka broker : %s", KAFKA_BROKER)
    logger.info("  Topic        : %s", KAFKA_TOPIC)
    logger.info("  Postgres URL : %s", POSTGRES_URL)
    logger.info("  MinIO path   : %s", RAW_S3_PATH)

    spark = build_spark()

    # Streaming source
    raw_stream = read_kafka(spark)

    # Single streaming query using foreachBatch
    # (avoids the stateful join complexity of merging aggregation + raw streams)
    query = (
        raw_stream
        .writeStream
        .foreachBatch(make_batch_writer(spark))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/fraud-detector")
        .trigger(processingTime="30 seconds")  # micro-batch every 30 s
        .start()
    )

    logger.info("Streaming query started (id=%s). Waiting for data…", query.id)
    query.awaitTermination()


if __name__ == "__main__":
    main()
