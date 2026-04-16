with clients as (
    select * from {{ ref('stg_clients') }}
),

dispositions as (
    select * from {{ ref('stg_dispositions') }}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
),
-- JOINING ALL TABLES NEEDED TO GET CUSTOMER-ACCOUNT RELATIONSHIP
customers_with_accounts as (
    select
        c.client_id,
        c.gender,
        c.date_of_birth,
        -- Age at current date (note: dataset is from 1993-1998)
        date_diff(current_date(), c.date_of_birth, year)  as age,
        d.account_id,
        d.disposition_type as account_role,
        a.frequency,
        a.account_created_on
    from clients c
    left join dispositions d using (client_id)
    left join accounts a using (account_id)
)

select * from customers_with_accounts