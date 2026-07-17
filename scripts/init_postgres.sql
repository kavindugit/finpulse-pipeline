-- =============================================================================
-- FinPulse — Postgres schema initialisation
-- =============================================================================
-- Run once (or re-run safely; all statements are idempotent).
-- The 'airflow' database is managed by Apache Airflow.
-- We create a separate 'finpulse' database for application data so that
-- Airflow metadata and pipeline data remain isolated.
-- =============================================================================

-- Create the application database (skip if it already exists)
SELECT 'CREATE DATABASE finpulse'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'finpulse')\gexec

\connect finpulse

-- ---------------------------------------------------------------------------
-- flagged_transactions
-- ---------------------------------------------------------------------------
-- Written by the Spark Structured Streaming job (fraud_detector.py).
-- Contains every transaction that triggered at least one fraud rule.
--
-- Design decisions:
--   * PRIMARY KEY on transaction_id → idempotent upserts; re-running the
--     Spark job after a crash will not duplicate rows.
--   * Separate 'finpulse' DB → dashboard queries don't compete with Airflow.
--   * Indexes on event_time, rule_name, origin_account → fast dashboard
--     queries and efficient time-range scans.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS flagged_transactions (
    transaction_id   TEXT            PRIMARY KEY,
    event_time       TIMESTAMPTZ     NOT NULL,
    tx_type          TEXT,
    amount           NUMERIC(18, 2),
    origin_account   TEXT,
    dest_account     TEXT,
    rule_name        TEXT,               -- e.g. 'rapid_fire', 'large_amount'
    tx_count_5m      INTEGER,            -- window context (for rapid_fire)
    total_amount_5m  NUMERIC(18, 2),     -- window context
    max_amount_5m    NUMERIC(18, 2),     -- window context
    geo_impossible   SMALLINT DEFAULT 0,
    detected_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ft_event_time
    ON flagged_transactions (event_time DESC);

CREATE INDEX IF NOT EXISTS idx_ft_rule
    ON flagged_transactions (rule_name);

CREATE INDEX IF NOT EXISTS idx_ft_origin
    ON flagged_transactions (origin_account);

-- ---------------------------------------------------------------------------
-- raw_transactions (optional lightweight mirror for ad-hoc queries)
-- ---------------------------------------------------------------------------
-- The canonical raw store is MinIO (s3a://finpulse/raw/).
-- This view-friendly table is written by the Spark job only when
-- WRITE_RAW_TO_POSTGRES=true (default: false — MinIO is the raw sink).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id      TEXT            PRIMARY KEY,
    event_time          TIMESTAMPTZ     NOT NULL,
    ingestion_time      TIMESTAMPTZ,
    tx_type             TEXT,
    amount              NUMERIC(18, 2),
    origin_account      TEXT,
    old_balance_orig    NUMERIC(18, 2),
    new_balance_orig    NUMERIC(18, 2),
    dest_account        TEXT,
    old_balance_dest    NUMERIC(18, 2),
    new_balance_dest    NUMERIC(18, 2),
    location            TEXT,
    is_fraud            SMALLINT DEFAULT 0,
    anomaly_type        TEXT
);

CREATE INDEX IF NOT EXISTS idx_rt_event_time
    ON raw_transactions (event_time DESC);

-- ---------------------------------------------------------------------------
-- daily_fraud_summary
-- ---------------------------------------------------------------------------
-- Written by the Airflow reconciliation task (reconcile_streaming.py).
-- Stores pre-aggregated daily fraud statistics derived from MinIO raw Parquet.
-- Used by the dashboard to plot daily trend lines without scanning Parquet.
-- PK on summary_date guarantees idempotent upserts (re-running the DAG
-- for the same date just overwrites the row, never duplicates it).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS daily_fraud_summary (
    summary_date      DATE            PRIMARY KEY,
    total_txns        INTEGER         NOT NULL DEFAULT 0,
    flagged_count     INTEGER         NOT NULL DEFAULT 0,
    total_amount      NUMERIC(20, 2),
    flagged_amount    NUMERIC(20, 2),
    top_rule          TEXT,               -- rule that fired most often that day
    computed_at       TIMESTAMPTZ     DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dfs_date
    ON daily_fraud_summary (summary_date DESC);

-- ---------------------------------------------------------------------------
-- kaggle_loans
-- ---------------------------------------------------------------------------
-- Written by the Airflow ingest task (load_kaggle_loans.py).
-- Source: Kaggle credit-risk-dataset (bundled sample CSV for portability).
-- Stored in MinIO as s3a://finpulse/reference/loans/ and mirrored here.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS kaggle_loans (
    loan_id           TEXT            PRIMARY KEY,
    person_age        SMALLINT,
    person_income     NUMERIC(14, 2),
    loan_amnt         NUMERIC(14, 2),
    loan_int_rate     NUMERIC(6, 3),
    loan_grade        TEXT,
    loan_intent       TEXT,
    loan_status       SMALLINT,           -- 0 = non-default, 1 = default
    cb_person_default_on_file TEXT,
    loaded_at         TIMESTAMPTZ     DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_kl_grade
    ON kaggle_loans (loan_grade);

CREATE INDEX IF NOT EXISTS idx_kl_status
    ON kaggle_loans (loan_status);

-- ---------------------------------------------------------------------------
-- cbsl_exchange_rates
-- ---------------------------------------------------------------------------
-- Written by the Airflow ingest task (load_cbsl_rates.py).
-- Source: open.er-api.com (free, no-key USD/LKR daily rate).
-- Stored in MinIO as s3a://finpulse/reference/cbsl/dt=<date>/ and mirrored here.
-- PK on rate_date → safe to re-run the DAG without duplicating rows.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS cbsl_exchange_rates (
    rate_date         DATE            PRIMARY KEY,
    usd_lkr_rate      NUMERIC(10, 4)  NOT NULL,
    source_url        TEXT,
    loaded_at         TIMESTAMPTZ     DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cer_date
    ON cbsl_exchange_rates (rate_date DESC);
