/*
  stg_kaggle_loans.sql
  ─────────────────────
  Staging model: 1:1 clean-up of the kaggle_loans source table.

  Transformations:
    - Map loan_status integer → human-readable text for dashboards
    - Normalize loan_grade to uppercase
    - Keep original numeric columns for mart aggregations

  Materialization: view
*/

with source as (
    select * from {{ source('finpulse_raw', 'kaggle_loans') }}
),

renamed as (
    select
        loan_id,
        coalesce(person_age, 0)             as person_age,
        coalesce(person_income, 0)          as person_income,
        coalesce(loan_amnt, 0)              as loan_amnt,
        coalesce(loan_int_rate, 0)          as loan_int_rate,
        upper(loan_grade)                   as loan_grade,
        loan_intent,
        loan_status,

        -- Human-readable label (0 = performing, 1 = default)
        case loan_status
            when 1 then 'Default'
            else         'Performing'
        end                                 as loan_status_label,

        cb_person_default_on_file,
        loaded_at

    from source
)

select * from renamed
