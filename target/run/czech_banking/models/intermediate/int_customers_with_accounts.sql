

  create or replace view `czech-banking-analysis`.`dbt_dev`.`int_customers_with_accounts`
  OPTIONS()
  as with clients as (
    select * from `czech-banking-analysis`.`dbt_dev`.`stg_clients`
),

dispositions as (
    select * from `czech-banking-analysis`.`dbt_dev`.`stg_dispositions`
),

accounts as (
    select * from `czech-banking-analysis`.`dbt_dev`.`stg_accounts`
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
    -- After running dbt test, duplicate loan_id rows were flagged in fct_loans (145 duplicates).
    -- Root cause: one account can have multiple clients linked to it (OWNER + DISPONENT).
    -- When joining to fct_loans on account_id, this created one row per client per loan, instead of one row per loan.
    -- Fix: filter to OWNER only so each account maps to exactly one client, ensuring loan_id remains unique in fct_loans.
    where d.disposition_type = 'OWNER'
)

select * from customers_with_accounts;

