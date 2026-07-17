/*
  fact_fraud_events.sql
  ──────────────────────
  Central fact table of the star schema.
  One row per flagged fraud event, enriched with:
    - Foreign keys to dim_date, dim_account, dim_rule
    - LKR-converted amount (joined from the latest CBSL exchange rate)
    - Window context from the Spark job (tx_count_5m, total_amount_5m)

  Why we build this:
    This is the single table every Superset chart queries for transaction-level
    metrics. Pre-joining the dimensions and pre-converting currency here means
    the dashboard never has to do expensive multi-table joins at query time.

  Materialization: table (in marts schema)
*/

with flagged as (
    select * from {{ ref('stg_flagged_transactions') }}
),

-- Get the most recent exchange rate available (use latest if today's not yet loaded)
latest_rate as (
    select usd_lkr_rate
    from {{ ref('stg_cbsl_exchange_rates') }}
    order by rate_date desc
    limit 1
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

dim_account as (
    select * from {{ ref('dim_account') }}
),

dim_rule as (
    select * from {{ ref('dim_rule') }}
),

fact as (
    select
        -- Natural key
        f.transaction_id,

        -- Foreign keys for star schema joins
        to_char(f.event_date, 'YYYYMMDD')::integer  as date_key,
        md5(f.origin_account)                        as account_key,
        md5(f.rule_name)                             as rule_key,

        -- Degenerate dimensions (stored on the fact row for convenience)
        f.tx_type,
        f.rule_name,
        f.origin_account,
        f.dest_account,

        -- Measures
        f.amount                                     as amount_usd,
        round(f.amount * r.usd_lkr_rate, 2)         as amount_lkr,
        f.tx_count_5m,
        f.total_amount_5m,
        f.max_amount_5m,
        f.geo_impossible,

        -- Timestamps
        f.event_time,
        f.event_date,
        f.detected_at

    from flagged f
    cross join latest_rate r
)

select * from fact
