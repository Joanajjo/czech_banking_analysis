with loans as (
    select * from {{ ref('stg_loans') }}
),

accounts_enriched as (
    select * from {{ ref('int_accounts_enriched') }}
),

customers as (
    select
        account_id,
        client_id,
        gender,
        date_of_birth,
        age
    from {{ ref('int_customers_with_accounts') }}
),

fct_loans as (
    select
        l.loan_id,
        l.account_id,
        l.loan_date,
        l.loan_amount,
        l.loan_duration_months,
        l.monthly_payment,
        l.loan_status,
        l.loan_status_label,
        l.loan_active_status,
        l.is_default_risk,
        a.district_id,
        a.district_name,
        a.region,
        a.avg_salary,
        a.unemployment_rate_95,
        a.crime_rate_95,
        a.total_transactions,
        a.total_credits,
        a.total_debits,
        a.avg_monthly_balance,
        a.last_transaction_date,
        c.client_id,
        c.gender,
        c.date_of_birth,
        c.age
    from loans l
    left join accounts_enriched a using (account_id)
    left join customers c using (account_id)
)

select * from fct_loans