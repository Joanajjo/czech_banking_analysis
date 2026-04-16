

  create or replace view `czech-banking-analysis`.`dbt_dev`.`stg_loans`
  OPTIONS()
  as with source as (
    select * from `czech-banking-analysis`.`raw`.`loans`
),
renamed as (
    select
        loan_id,
        account_id,
        parse_date('%y%m%d', cast(date as string)) as loan_date,
        amount as loan_amount,
        duration as loan_duration_months,
        payments as monthly_payment,
        status as loan_status,
        -- Status A and B are finished loans. Assuming that by seeing old dates and shorter durations. C and D are active loans (recent dates and longer durations in general), with D being in debt.
        case status
            when 'A' then 'Finished - No Problems'
            when 'B' then 'Finished - Unpaid'
            when 'C' then 'Running - OK'
            when 'D' then 'Running - In Debt'
        end as loan_status_label,

        -- Active flag based on your earliest loan and lastest loan dates. These range between 93/07-94/07 and 97/09 and 98/12.
        case status
            when 'A' then 'Inactive'
            when 'B' then 'Inactive'
            when 'C' then 'Active'
            when 'D' then 'Active'
        end as loan_active_status,

        -- Risk flag! Assuming that the loans in status B and D are at risk, as they are either unpaid or in debt.
        case
            when status in ('B', 'D') then true
            else false
        end as is_default_risk
    from source
)
select * from renamed;

