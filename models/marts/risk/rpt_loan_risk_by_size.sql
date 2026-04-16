-- Answers Q4: Is there a loan amount or duration threshold above which default risk increases significantly?
-- Granularity: one row per loan amount bucket

with loan_buckets as (
    select
        -- Bucketing loan amounts into three bands to see if larger loans have higher default rates. The buckets are defined as: Small: under 100,000 CZK, Medium: 100,000 - 200,000 CZK, Large: over 200,000 CZK
        case
            when loan_amount < 100000  then '1_Small (under 100k)'
            when loan_amount < 200000  then '2_Medium (100k-200k)'
            else                            '3_Large (over 200k)'
        end                                             as loan_size_bucket,

        -- Using actual duration values (confirmed in BigQuery only 5 exist): 12, 24, 36, 48, 60 months
        loan_duration_months,
        loan_id,
        loan_amount,
        monthly_payment,
        is_default_risk,
        loan_active_status,
        avg_salary,
        unemployment_rate_95

    from {{ ref('fct_loans') }}
),

aggregated as (
    select
        loan_size_bucket,
        loan_duration_months,
        count(loan_id)                                              as total_loans,
        countif(is_default_risk = true)                             as total_defaults,
        round(countif(is_default_risk = true) * 100.0
              / count(loan_id), 1)                                  as default_rate_pct,
        round(avg(loan_amount), 0)                                  as avg_loan_amount,
        round(min(loan_amount), 0)                                  as min_loan_amount,
        round(max(loan_amount), 0)                                  as max_loan_amount,
        round(avg(monthly_payment), 0)                              as avg_monthly_payment,
        round(avg(safe_cast(avg_salary as float64)), 0)             as avg_district_salary,
        round(avg(safe_cast(unemployment_rate_95 as float64)), 2)   as avg_unemployment
    from loan_buckets
    group by loan_size_bucket, loan_duration_months
)

select * from aggregated
order by loan_size_bucket, loan_duration_months