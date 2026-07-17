/*
  mart_loan_risk.sql
  ───────────────────
  Loan risk mart: aggregates the Kaggle credit-risk dataset by loan grade,
  enriched with the latest USD/LKR rate to express loan amounts in LKR.

  Powered by:
    - stg_kaggle_loans      → credit risk attributes per loan
    - stg_cbsl_exchange_rates → USD/LKR conversion

  Why we build this:
    The "Loan Default Rate by Grade" Superset chart needs one row per loan
    grade (A/B/C/D/E/F/G) with aggregated default rate and average loan
    amount. Without this mart, the chart would need complex SQL that most
    BI tools handle awkwardly.

  Materialization: table
*/

with loans as (
    select * from {{ ref('stg_kaggle_loans') }}
),

latest_rate as (
    select usd_lkr_rate
    from {{ ref('stg_cbsl_exchange_rates') }}
    order by rate_date desc
    limit 1
),

by_grade as (
    select
        l.loan_grade,
        l.loan_intent,

        -- Volume
        count(*)                                            as total_loans,
        sum(l.loan_status)                                  as total_defaults,

        -- Default rate as percentage
        round(
            sum(l.loan_status)::numeric / count(*) * 100, 2
        )                                                   as default_rate_pct,

        -- Loan amount stats (USD and LKR)
        round(avg(l.loan_amnt), 2)                          as avg_loan_amnt_usd,
        round(avg(l.loan_amnt) * r.usd_lkr_rate, 2)        as avg_loan_amnt_lkr,
        round(avg(l.loan_int_rate), 3)                      as avg_interest_rate,
        round(avg(l.person_income), 2)                      as avg_person_income,

        -- Exchange rate used (for auditability)
        r.usd_lkr_rate

    from loans l
    cross join latest_rate r
    group by l.loan_grade, l.loan_intent, r.usd_lkr_rate
)

select * from by_grade
order by loan_grade, loan_intent
