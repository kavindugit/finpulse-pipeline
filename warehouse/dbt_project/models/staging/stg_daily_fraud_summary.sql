/*
  stg_daily_fraud_summary.sql
  ────────────────────────────
  Staging model: 1:1 clean-up of the daily_fraud_summary source table.

  This table is already highly aggregated (one row per day) so there is
  very little transformation needed. We rename for consistency and
  compute a fraud_rate column for convenience.

  Materialization: view
*/

with source as (
    select * from {{ source('finpulse_raw', 'daily_fraud_summary') }}
),

renamed as (
    select
        summary_date,
        total_txns,
        flagged_count,
        coalesce(total_amount, 0)           as total_amount,
        coalesce(flagged_amount, 0)         as flagged_amount,
        top_rule,
        computed_at,

        -- Derived metric: what % of transactions were flagged that day?
        case
            when total_txns = 0 then 0
            else round(flagged_count::numeric / total_txns * 100, 2)
        end                                 as fraud_rate_pct

    from source
)

select * from renamed
