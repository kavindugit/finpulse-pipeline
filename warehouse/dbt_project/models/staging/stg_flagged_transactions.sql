/*
  stg_flagged_transactions.sql
  ─────────────────────────────
  Staging model: 1:1 clean-up of the flagged_transactions source table.

  Why this exists:
    Staging models act as a firewall between the raw source schema and all
    downstream logic. If Spark renames a column tomorrow, we fix it here in
    one place and every mart model stays untouched.

  Transformations applied here:
    - Explicit column aliases (snake_case → consistent naming)
    - COALESCE to handle NULLs in optional fields
    - Cast event_time to DATE for easier joins to dim_date
    - No business logic — this model must be a straightforward read.

  Materialization: view (set in dbt_project.yml) — always reflects latest data.
*/

with source as (
    select * from {{ source('finpulse_raw', 'flagged_transactions') }}
),

renamed as (
    select
        -- Identifiers
        transaction_id,
        origin_account,
        dest_account,

        -- Timestamps
        event_time,
        event_time::date                    as event_date,
        detected_at,

        -- Transaction attributes
        tx_type,
        amount,

        -- Fraud detection context
        rule_name,
        coalesce(tx_count_5m, 0)            as tx_count_5m,
        coalesce(total_amount_5m, 0)        as total_amount_5m,
        coalesce(max_amount_5m, 0)          as max_amount_5m,
        coalesce(geo_impossible, 0)         as geo_impossible

    from source
)

select * from renamed
