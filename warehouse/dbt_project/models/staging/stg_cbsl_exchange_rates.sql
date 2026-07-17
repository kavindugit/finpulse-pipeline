/*
  stg_cbsl_exchange_rates.sql
  ────────────────────────────
  Staging model: 1:1 clean-up of the cbsl_exchange_rates source table.

  This is the simplest staging model — the source is already one row per
  day with a single rate column. We expose it cleanly for use in mart
  models that need to convert USD amounts to LKR.

  Materialization: view
*/

with source as (
    select * from {{ source('finpulse_raw', 'cbsl_exchange_rates') }}
),

renamed as (
    select
        rate_date,
        usd_lkr_rate,
        source_url,
        loaded_at
    from source
)

select * from renamed
