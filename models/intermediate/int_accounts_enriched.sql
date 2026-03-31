with accounts as (
    select * from {{ ref('stg_accounts') }}
),
districts as (
    select * from {{ ref('stg_districts') }}
),
transaction_summary as (
    select
        account_id,
        count(*)                                  as total_transactions,
        sum(case when transaction_type = 'PRIJEM' then transaction_amount else 0 end) as total_credits,
        sum(case when transaction_type = 'VYDAJ'  then transaction_amount else 0 end) as total_debits,
        min(transaction_date)                     as first_transaction_date,
        max(transaction_date)                     as last_transaction_date
    from {{ ref('stg_transactions') }}
    group by 1
)
select
    a.account_id, a.district_id, a.frequency,
    d.district_name, d.unemployment_rate_95,
    t.total_transactions, t.total_credits, t.total_debits,
    t.total_credits - t.total_debits as net_balance_from_transactions,
    t.first_transaction_date, t.last_transaction_date
from accounts a
left join districts d on a.district_id = d.district_id
