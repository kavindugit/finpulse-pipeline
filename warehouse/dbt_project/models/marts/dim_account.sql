/*
  dim_account.sql
  ────────────────
  Account dimension: one row per unique origin account seen in flagged
  transactions, with pre-aggregated risk metrics.

  Why we build this:
    The "Top 10 High-Risk Accounts" Superset chart queries this dimension
    directly. Without it, every dashboard refresh would scan all flagged
    transactions and aggregate on the fly — slow and expensive.

  Materialization: table
*/

with flagged as (
    select * from {{ ref('stg_flagged_transactions') }}
),

accounts as (
    select
        -- Surrogate key: MD5 hash of account ID
        md5(origin_account)                         as account_key,
        origin_account                              as account_id,

        -- Risk profile metrics
        count(*)                                    as total_flags,
        sum(amount)                                 as total_flagged_amount,
        avg(amount)                                 as avg_flagged_amount,
        max(amount)                                 as max_single_flagged_amount,
        min(event_time)                             as first_seen_at,
        max(event_time)                             as last_seen_at,

        -- Which rule fires most against this account?
        mode() within group (order by rule_name)    as most_common_rule,

        -- How many distinct rules have fired on this account?
        count(distinct rule_name)                   as distinct_rules_triggered

    from flagged
    group by origin_account
)

select * from accounts
