with accounts as (
    select * from {{ ref('stg_accounts') }}
),

districts as (
    select * from {{ ref('stg_districts') }}
),

transaction_summary as (
    select
        account_id,
        count(*) as total_transactions,
        sum(case when transaction_type = 'PRIJEM'
                 then transaction_amount else 0 end) as total_credits,
        sum(case when transaction_type = 'VYDAJ'
                 then transaction_amount else 0 end) as total_debits,
        avg(balance_after_transaction) as avg_monthly_balance,
        max(transaction_date) as last_transaction_date
    from {{ ref('stg_transactions') }}
    group by account_id
),

accounts_enriched as (
    select
        a.account_id,
        a.district_id,
        a.frequency,
        a.account_created_on,
        d.district_name,
        d.region,
        d.avg_salary,
        d.unemployment_rate_95,
        d.crime_rate_95,
        t.total_transactions,
        t.total_credits,
        t.total_debits,
        t.avg_monthly_balance,
        t.last_transaction_date
    from accounts a
    left join districts d using (district_id)
    left join transaction_summary t using (account_id)
)

select * from accounts_enriched