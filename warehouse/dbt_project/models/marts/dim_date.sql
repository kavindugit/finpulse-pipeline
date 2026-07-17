/*
  dim_date.sql
  ─────────────
  Date dimension: one row per unique date found in flagged_transactions.

  Why we build this:
    BI tools almost always need to slice metrics by time. Without a proper
    date dimension, every chart has to do its own date arithmetic. By
    pre-computing year/month/week/is_weekend here once, all downstream
    charts get those attributes for free.

  In production this table would cover a full date range (e.g. 2020–2030).
  For the demo we only generate rows for dates that actually have data.

  Materialization: table (set in dbt_project.yml)
*/

with date_spine as (
    -- Pull distinct dates directly from our flagged transactions
    select distinct event_date as calendar_date
    from {{ ref('stg_flagged_transactions') }}
),

enriched as (
    select
        -- Surrogate key: YYYYMMDD integer — compact, sort-friendly
        to_char(calendar_date, 'YYYYMMDD')::integer     as date_key,

        calendar_date                                   as full_date,
        extract(year  from calendar_date)::integer      as year,
        extract(month from calendar_date)::integer      as month,
        extract(day   from calendar_date)::integer      as day,
        extract(dow   from calendar_date)::integer      as day_of_week,  -- 0=Sun, 6=Sat
        to_char(calendar_date, 'Day')                   as day_name,
        to_char(calendar_date, 'Month')                 as month_name,
        extract(quarter from calendar_date)::integer    as quarter,
        extract(week  from calendar_date)::integer      as week_of_year,

        -- Useful boolean flags
        case when extract(dow from calendar_date) in (0, 6)
             then true else false end                   as is_weekend,

        -- For BI filtering: "2026-07" style label
        to_char(calendar_date, 'YYYY-MM')               as year_month

    from date_spine
)

select * from enriched
