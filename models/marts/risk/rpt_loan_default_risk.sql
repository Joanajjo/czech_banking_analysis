-- Answers Q1: What customer and account characteristics are most associated with loan default?
-- Granularity: one row per combination of gender, region, and loan active status.

with loan_default_risk as (
    select
        -- Dimensions we are grouping by — the characteristics we want to analyse
        gender,
        region,
        loan_active_status,

        -- Volume metrics
        count(loan_id) as total_loans,
        countif(is_default_risk = true) as total_defaults,

        -- Default rate = defaults / total loans * 100
        -- The 100.0 forces a decimal division instead of integer division
        round(countif(is_default_risk = true) * 100.0 
              / count(loan_id), 1) as default_rate_pct,

        -- Loan characteristics to answer: Do defaulters borrow more or for longer?
        round(avg(loan_amount), 0) as avg_loan_amount,
        round(avg(loan_duration_months), 0) as avg_duration_months,
        round(avg(monthly_payment), 0) as avg_monthly_payment,

        -- Customer characteristics to answer: Does age correlate with default?
        -- Using age_at_loan_date not current age — dataset is from 1993-1998
        round(avg(age_at_loan_date), 0) as avg_age_at_loan_date,

        -- Regional characteristics to answer: Does the local economy and safety explain default rates?
        round(avg(safe_cast(unemployment_rate_95 as float64)), 2) as avg_unemployment,
        round(avg(safe_cast(crime_rate_95 as float64)), 2) as avg_crime_rate,
        round(avg(safe_cast(avg_salary as float64)), 0) as avg_district_salary,

        -- Account activity to answer: Does engagement predict financial health?
        round(avg(total_transactions), 0) as avg_transactions_per_account,
        round(avg(total_credits), 0) as avg_total_credits,
        round(avg(total_debits), 0) as avg_total_debits

    from {{ ref('fct_loans') }}

    -- Grouping by these three dimensions to let us compare default rates across gender, geography, and whether the loan is still active or not.
    group by gender, region, loan_active_status
)

select * from loan_default_risk