"""
reconcile_streaming.py
-----------------------
Airflow task function: daily reconciliation of the streaming pipeline output.

Reads the previous day's raw Parquet files from MinIO (the bronze layer
written by the Spark Structured Streaming job), aggregates fraud statistics,
and upserts a single summary row into the daily_fraud_summary Postgres table.

This bridges the streaming and batch worlds:
  - Streaming side  → writes every raw transaction to MinIO continuously.
  - Batch side      → once a day, summarises yesterday's data for dashboards.

Idempotency:
  - Postgres upsert on PRIMARY KEY (summary_date): re-running for the same
    date overwrites — never duplicates.

MinIO path convention (matches fraud_detector.py):
  s3a://finpulse/raw/dt=YYYY-MM-DD/part-*.parquet
"""

from __future__ import annotations

import io
import logging
import os
from datetime import date, datetime, timedelta, timezone

import boto3
import pandas as pd
import psycopg2
from botocore.client import Config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
S3_ENDPOINT   = os.getenv("S3_ENDPOINT",           "http://minio:9000")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID",      "minioadmin")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY",  "minioadmin")
S3_BUCKET     = os.getenv("S3_BUCKET",              "finpulse")
RAW_PREFIX    = "raw"

POSTGRES_HOST = os.getenv("POSTGRES_HOST",          "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT",      "5432"))
POSTGRES_DB   = os.getenv("POSTGRES_DB",            "finpulse")
POSTGRES_USER = os.getenv("POSTGRES_USER",          "airflow")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD",      "airflow")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _list_parquet_keys(client, summary_date: date) -> list[str]:
    """List all Parquet object keys under raw/dt=<date>/."""
    prefix = f"{RAW_PREFIX}/dt={summary_date}/"
    resp = client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    keys = [
        obj["Key"]
        for obj in resp.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
    logger.info("Found %d Parquet files for %s in MinIO.", len(keys), summary_date)
    return keys


def _read_parquet_from_minio(client, key: str) -> pd.DataFrame:
    resp = client.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(resp["Body"].read()))


def _aggregate(df: pd.DataFrame) -> dict:
    """
    Compute summary statistics from the raw transactions DataFrame.

    Returns a dict matching the daily_fraud_summary schema columns.
    """
    total_txns     = len(df)
    total_amount   = float(df["amount"].sum()) if "amount" in df.columns else 0.0

    # is_fraud_detected may come from the Spark job's scored column
    fraud_col = "is_fraud_detected" if "is_fraud_detected" in df.columns else None
    if fraud_col:
        flagged     = df[df[fraud_col] == True]   # noqa: E712
        flagged_cnt = len(flagged)
        flagged_amt = float(flagged["amount"].sum()) if len(flagged) else 0.0

        # Most common fraud rule today
        top_rule = None
        if flagged_cnt > 0 and "rule_name" in flagged.columns:
            mode_series = flagged["rule_name"].dropna().mode()
            top_rule = str(mode_series.iloc[0]) if len(mode_series) else None
    else:
        flagged_cnt = 0
        flagged_amt = 0.0
        top_rule    = None

    return {
        "total_txns":    total_txns,
        "flagged_count": flagged_cnt,
        "total_amount":  total_amount,
        "flagged_amount": flagged_amt,
        "top_rule":      top_rule,
    }


def _upsert_postgres(summary_date: date, stats: dict) -> None:
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASS,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO daily_fraud_summary
                    (summary_date, total_txns, flagged_count,
                     total_amount, flagged_amount, top_rule, computed_at)
                VALUES (%s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (summary_date) DO UPDATE SET
                    total_txns    = EXCLUDED.total_txns,
                    flagged_count = EXCLUDED.flagged_count,
                    total_amount  = EXCLUDED.total_amount,
                    flagged_amount= EXCLUDED.flagged_amount,
                    top_rule      = EXCLUDED.top_rule,
                    computed_at   = now();
                """,
                (
                    summary_date,
                    stats["total_txns"],
                    stats["flagged_count"],
                    stats["total_amount"],
                    stats["flagged_amount"],
                    stats["top_rule"],
                ),
            )
            logger.info(
                "Postgres: upserted daily_fraud_summary for %s → %d txns, %d flagged.",
                summary_date, stats["total_txns"], stats["flagged_count"],
            )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point — called by Airflow PythonOperator
# ---------------------------------------------------------------------------
def run(**context) -> None:
    """
    Main callable for the Airflow PythonOperator.

    Processes data_interval_start's date (yesterday when running @daily).
    When run standalone (no Airflow context) it uses yesterday's UTC date.
    """
    # Airflow passes ds as YYYY-MM-DD (the DAG's logical date = start of interval)
    ds_str: str | None = context.get("ds")
    if ds_str:
        summary_date = date.fromisoformat(ds_str)
    else:
        summary_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    logger.info("Reconciling streaming data for date: %s", summary_date)

    client = _s3_client()
    keys   = _list_parquet_keys(client, summary_date)

    if not keys:
        # No data for this date — write a zero-row summary so the dashboard
        # shows a gap rather than a missing date.
        logger.warning(
            "No raw Parquet files found for %s — inserting zero-row summary.",
            summary_date,
        )
        _upsert_postgres(summary_date, {
            "total_txns": 0, "flagged_count": 0,
            "total_amount": 0.0, "flagged_amount": 0.0, "top_rule": None,
        })
        return

    # Read and concatenate all Parquet shards for this date
    frames = [_read_parquet_from_minio(client, k) for k in keys]
    df     = pd.concat(frames, ignore_index=True)
    logger.info("Read %d total rows from MinIO for %s.", len(df), summary_date)

    stats = _aggregate(df)
    _upsert_postgres(summary_date, stats)
    logger.info("reconcile_streaming_data completed for %s.", summary_date)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    run()
