"""
daily_reconciliation.py
------------------------
FinPulse — Airflow DAG: daily_finpulse_pipeline

Runs once a day and orchestrates three tasks in sequence:

  Task 1 ─ reconcile_streaming_data
      Reads yesterday's raw Parquet from MinIO (bronze layer written by
      the Spark Structured Streaming job), aggregates fraud statistics,
      and upserts one summary row into the daily_fraud_summary Postgres table.

  Task 2 ─ ingest_kaggle_loans
      Loads a bundled Kaggle credit-risk dataset (CSV), writes it to MinIO
      as Parquet under reference/loans/, and upserts rows into kaggle_loans.

  Task 3 ─ ingest_cbsl_rates
      Fetches the current USD/LKR exchange rate from open.er-api.com,
      writes Parquet to reference/cbsl/dt=<date>/, and upserts one row
      into cbsl_exchange_rates.

Design decisions
----------------
* All three tasks use INSERT … ON CONFLICT DO UPDATE — idempotent on any
  natural key — so re-triggering a failed or partially-completed run never
  produces duplicate rows.

* Tasks 2 and 3 run in parallel after task 1 completes, because they are
  independent of each other.

* catchup=False prevents Airflow from backfilling every day since
  start_date on first deployment — important for portfolio demos.

* max_active_runs=1 prevents two runs from clobbering each other.

Environment variables expected in Airflow workers (injected via docker-compose):
  S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

from __future__ import annotations

import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from ingest.reconcile_streaming import run as reconcile_streaming
from ingest.load_kaggle_loans   import run as load_kaggle_loans
from ingest.load_cbsl_rates     import run as load_cbsl_rates

# ---------------------------------------------------------------------------
# Default task arguments — applied to every task unless overridden
# ---------------------------------------------------------------------------
default_args = {
    "owner":            "finpulse",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="daily_finpulse_pipeline",
    description=(
        "Reconcile streaming output + ingest Kaggle loan data "
        "and CBSL exchange rates into the FinPulse data platform."
    ),
    schedule_interval="@daily",          # midnight UTC
    start_date=datetime(2026, 7, 17),    # avoids backfill on first deploy
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["finpulse", "batch", "reconciliation"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 1: Reconcile streaming data
    # -----------------------------------------------------------------------
    t1_reconcile = PythonOperator(
        task_id="reconcile_streaming_data",
        python_callable=reconcile_streaming,
        doc_md="""\
        **reconcile_streaming_data**

        Reads MinIO `raw/dt={{ ds }}/` Parquet files (written by the
        Spark Structured Streaming job), aggregates total transactions,
        fraud count, amounts, and top fraud rule, then upserts a single
        row into `daily_fraud_summary` for date `{{ ds }}`.

        Safe to retry: upsert is idempotent on `summary_date` PK.
        """,
    )

    # -----------------------------------------------------------------------
    # Task 2: Ingest Kaggle credit-risk loans dataset
    # -----------------------------------------------------------------------
    t2_loans = PythonOperator(
        task_id="ingest_kaggle_loans",
        python_callable=load_kaggle_loans,
        doc_md="""\
        **ingest_kaggle_loans**

        Reads a bundled 100-row sample of the Kaggle Credit Risk Dataset
        (no credentials needed), uploads it as Parquet to
        `s3://finpulse/reference/loans/`, and upserts rows into the
        `kaggle_loans` Postgres table.

        Safe to retry: MinIO upload is skipped if object already exists;
        Postgres write uses ON CONFLICT DO UPDATE.
        """,
    )

    # -----------------------------------------------------------------------
    # Task 3: Ingest CBSL USD/LKR exchange rates
    # -----------------------------------------------------------------------
    t3_cbsl = PythonOperator(
        task_id="ingest_cbsl_rates",
        python_callable=load_cbsl_rates,
        doc_md="""\
        **ingest_cbsl_rates**

        Fetches today's USD/LKR rate from open.er-api.com (fallback to
        300.0 LKR if unreachable), writes a Parquet file to
        `s3://finpulse/reference/cbsl/dt={{ ds }}/`, and upserts one row
        into `cbsl_exchange_rates`.

        Safe to retry: upsert is idempotent on `rate_date` PK.
        """,
    )

    # -----------------------------------------------------------------------
    # Dependency graph:
    #   t1_reconcile → t2_loans
    #                → t3_cbsl
    # Tasks 2 and 3 are independent and run in parallel after task 1.
    # -----------------------------------------------------------------------
    t1_reconcile >> [t2_loans, t3_cbsl]
