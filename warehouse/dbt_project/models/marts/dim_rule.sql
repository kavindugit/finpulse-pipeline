/*
  dim_rule.sql
  ─────────────
  Rule dimension: one row per unique fraud rule name with a human-readable
  description of what the rule detects.

  Why we build this:
    The Superset "Fraud by Rule Breakdown" pie chart needs a label, not just
    a raw string like 'rapid_fire'. Having a description here also means an
    analyst can hover on a chart slice and understand what it means without
    reading the Spark source code.

  Materialization: table
*/

with rule_names as (
    -- Pull every distinct rule seen in the data
    select distinct rule_name
    from {{ ref('stg_flagged_transactions') }}
    where rule_name is not null
),

described as (
    select
        md5(rule_name)      as rule_key,
        rule_name,

        -- Map known rule names to human-readable descriptions
        case rule_name
            when 'rapid_fire'
                then 'Multiple transactions from the same account within 5 minutes'
            when 'large_amount'
                then 'Single transaction amount exceeds the account historical average by 10x'
            when 'odd_hour'
                then 'Transaction occurred between midnight and 5am local time'
            when 'geo_impossible'
                then 'Two consecutive transactions from geographically impossible locations'
            else
                -- Catch-all for any new rules added to the Spark job later
                initcap(replace(rule_name, '_', ' '))
        end                 as rule_description

    from rule_names
)

select * from described
