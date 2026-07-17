"""
load_cbsl_rates.py
-------------------
Airflow task function: fetch today's USD/LKR exchange rate and persist it.

Source: open.er-api.com — free, no API key required, 1 call/day is well
within their free tier limits.

Sinks:
  1. MinIO  → s3a://finpulse/reference/cbsl/dt=<date>/rates.parquet
  2. Postgres → cbsl_exchange_rates (upsert on rate_date)

Idempotency:
  - If the Parquet object already exists in MinIO it is overwritten with
    the same content (re-running the API call returns the same rate for the
    same calendar date).
  - Postgres write uses ON CONFLICT (rate_date) DO UPDATE.
"""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import date, datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError

import boto3
import pandas as pd
import psycopg2
from botocore.client import Config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ER_API_URL = "https://open.er-api.com/v6/latest/USD"
FALLBACK_LKR_RATE = 300.0          # used if the external API is unreachable

S3_ENDPOINT   = os.getenv("S3_ENDPOINT",           "http://minio:9000")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID",      "minioadmin")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY",  "minioadmin")
S3_BUCKET     = os.getenv("S3_BUCKET",              "finpulse")

POSTGRES_HOST = os.getenv("POSTGRES_HOST",          "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT",      "5432"))
POSTGRES_DB   = os.getenv("POSTGRES_DB",            "finpulse")
POSTGRES_USER = os.getenv("POSTGRES_USER",          "airflow")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD",      "airflow")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _fetch_usd_lkr(rate_date: date) -> tuple[float, str]:
    """
    Return (usd_lkr_rate, source_url).
    Falls back to FALLBACK_LKR_RATE if the API is unreachable.
    """
    try:
        req = Request(ER_API_URL, headers={"User-Agent": "FinPulsePipeline/1.0"})
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        rate = float(data["rates"]["LKR"])
        logger.info("Fetched USD/LKR = %.4f from %s", rate, ER_API_URL)
        return rate, ER_API_URL
    except URLError as exc:
        logger.warning("API unreachable (%s); using fallback rate %.4f", exc, FALLBACK_LKR_RATE)
        return FALLBACK_LKR_RATE, "fallback"


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _write_parquet_to_minio(df: pd.DataFrame, rate_date: date) -> None:
    key = f"reference/cbsl/dt={rate_date}/rates.parquet"
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    client = _s3_client()
    client.put_object(Bucket=S3_BUCKET, Key=key, Body=buffer.getvalue())
    logger.info("MinIO: uploaded → s3://%s/%s", S3_BUCKET, key)


def _upsert_postgres(rate_date: date, usd_lkr: float, source_url: str) -> None:
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASS,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cbsl_exchange_rates
                    (rate_date, usd_lkr_rate, source_url, loaded_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (rate_date) DO UPDATE SET
                    usd_lkr_rate = EXCLUDED.usd_lkr_rate,
                    source_url   = EXCLUDED.source_url,
                    loaded_at    = now();
                """,
                (rate_date, usd_lkr, source_url),
            )
            logger.info("Postgres: upserted rate for %s → %.4f LKR", rate_date, usd_lkr)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point — called by Airflow PythonOperator
# ---------------------------------------------------------------------------
def run(**context) -> None:
    """
    Main callable for the Airflow PythonOperator.

    Uses Airflow's logical date ({{ ds }}) when available so that
    back-filled runs fetch the correct historical date. Falls back to
    today's UTC date when run standalone.
    """
    # Airflow passes ds as a string 'YYYY-MM-DD' via the context dict
    ds_str: str | None = context.get("ds")
    if ds_str:
        rate_date = date.fromisoformat(ds_str)
    else:
        rate_date = datetime.now(timezone.utc).date()

    logger.info("Fetching USD/LKR rate for %s", rate_date)
    usd_lkr, source_url = _fetch_usd_lkr(rate_date)

    df = pd.DataFrame([{
        "rate_date":    str(rate_date),
        "usd_lkr_rate": usd_lkr,
        "source_url":   source_url,
        "loaded_at":    datetime.now(timezone.utc).isoformat(),
    }])

    _write_parquet_to_minio(df, rate_date)
    _upsert_postgres(rate_date, usd_lkr, source_url)
    logger.info("ingest_cbsl_rates completed successfully for %s.", rate_date)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    run()
