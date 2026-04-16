-- Answers Q2: Do high-activity accounts default less on loans?
-- Granularity: one row per default risk group (true = defaulted, false = no default)

with account_activity as (
    select
        is_default_risk,
        count(loan_id)                              as total_loans,

        -- Transaction activity: Do defaulters use their accounts less?
        round(avg(total_transactions), 0) as avg_transactions,
        round(avg(total_credits), 0) as avg_total_credits,
        round(avg(total_debits), 0) as avg_total_debits,

        -- Balance: Do defaulters have lower or higher balances?
        round(avg(avg_monthly_balance), 0)          as avg_monthly_balance,

        -- Loan characteristics: do defaulters borrow more?
        round(avg(loan_amount), 0) as avg_loan_amount,
        round(avg(loan_duration_months), 0) as avg_duration_months,
        round(avg(monthly_payment), 0) as avg_monthly_payment,

        -- Customer context: Do defaulters tend to be younger or older?
        round(avg(age_at_loan_date), 0) as avg_age_at_loan_date,

        -- Regional context to answer: Are defaulters in poorer areas?
        round(avg(safe_cast(avg_salary as float64)), 0) as avg_district_salary,
        round(avg(safe_cast(unemployment_rate_95 as float64)), 2) as avg_unemployment

    from {{ ref('fct_loans') }}
    group by is_default_risk
)

select * from account_activity