"""
load_kaggle_loans.py
---------------------
Airflow task function: ingest the Kaggle credit-risk loan dataset.

Reads a bundled sample CSV (no Kaggle credentials needed), writes it to
MinIO as Parquet under s3a://finpulse/reference/loans/, then upserts every
row into the kaggle_loans Postgres table.

Idempotency:
  - MinIO write: skips if object already exists (content-hash check).
  - Postgres write: INSERT ... ON CONFLICT (loan_id) DO UPDATE → safe to
    re-run any number of times without duplicating rows.
"""

from __future__ import annotations

import io
import logging
import os
from pathlib import Path

import boto3
import pandas as pd
import psycopg2
from botocore.client import Config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — all overridable via environment variables
# ---------------------------------------------------------------------------
S3_ENDPOINT       = os.getenv("S3_ENDPOINT",           "http://minio:9000")
S3_ACCESS_KEY     = os.getenv("AWS_ACCESS_KEY_ID",      "minioadmin")
S3_SECRET_KEY     = os.getenv("AWS_SECRET_ACCESS_KEY",  "minioadmin")
S3_BUCKET         = os.getenv("S3_BUCKET",              "finpulse")
S3_KEY            = "reference/loans/loans_sample.parquet"

POSTGRES_HOST     = os.getenv("POSTGRES_HOST",          "postgres")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT",      "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB",            "finpulse")
POSTGRES_USER     = os.getenv("POSTGRES_USER",          "airflow")
POSTGRES_PASS     = os.getenv("POSTGRES_PASSWORD",      "airflow")

# Bundled sample CSV — relative to this file so it works from any CWD
_SAMPLE_CSV = Path(__file__).parent / "data" / "loans_sample.csv"


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


def _object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except client.exceptions.ClientError:
        return False


def _write_parquet_to_minio(df: pd.DataFrame) -> None:
    """Convert DataFrame to Parquet in-memory and upload to MinIO."""
    client = _s3_client()

    if _object_exists(client, S3_BUCKET, S3_KEY):
        logger.info("MinIO: %s already exists — skipping upload (idempotent).", S3_KEY)
        return

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    client.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=buffer.getvalue())
    logger.info("MinIO: uploaded %d rows → s3://%s/%s", len(df), S3_BUCKET, S3_KEY)


def _upsert_postgres(df: pd.DataFrame) -> None:
    """Upsert loan rows into kaggle_loans; safe to re-run."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASS,
    )
    try:
        with conn, conn.cursor() as cur:
            upsert_sql = """
                INSERT INTO kaggle_loans
                    (loan_id, person_age, person_income, loan_amnt,
                     loan_int_rate, loan_grade, loan_intent,
                     loan_status, cb_person_default_on_file, loaded_at)
                VALUES
                    (%(loan_id)s, %(person_age)s, %(person_income)s,
                     %(loan_amnt)s, %(loan_int_rate)s, %(loan_grade)s,
                     %(loan_intent)s, %(loan_status)s,
                     %(cb_person_default_on_file)s, now())
                ON CONFLICT (loan_id) DO UPDATE SET
                    person_income   = EXCLUDED.person_income,
                    loan_amnt       = EXCLUDED.loan_amnt,
                    loan_int_rate   = EXCLUDED.loan_int_rate,
                    loan_grade      = EXCLUDED.loan_grade,
                    loan_intent     = EXCLUDED.loan_intent,
                    loan_status     = EXCLUDED.loan_status,
                    loaded_at       = now();
            """
            cur.executemany(upsert_sql, df.to_dict("records"))
            logger.info("Postgres: upserted %d rows into kaggle_loans.", len(df))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point — called by Airflow PythonOperator
# ---------------------------------------------------------------------------
def run(**context) -> None:
    """
    Main callable for the Airflow PythonOperator.

    Steps
    -----
    1. Load the bundled sample CSV.
    2. Light cleaning (strip whitespace, coerce types).
    3. Write Parquet to MinIO (skip if already uploaded).
    4. Upsert rows into Postgres kaggle_loans.
    """
    logger.info("Loading Kaggle loans from bundled CSV: %s", _SAMPLE_CSV)
    df = pd.read_csv(_SAMPLE_CSV, dtype=str)

    # Coerce numeric columns
    for col in ("person_age", "loan_status"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int16")
    for col in ("person_income", "loan_amnt", "loan_int_rate"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["loan_id"])
    logger.info("Loaded %d loan records.", len(df))

    _write_parquet_to_minio(df)
    _upsert_postgres(df)
    logger.info("ingest_kaggle_loans completed successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    run()
