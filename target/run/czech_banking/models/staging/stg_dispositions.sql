

  create or replace view `czech-banking-analysis`.`dbt_dev`.`stg_dispositions`
  OPTIONS()
  as with source as (
    select * from `czech-banking-analysis`.`raw`.`dispositions`
),
renamed as (
    select 
        disp_id as disposition_id,
        client_id,
        account_id,
        type as disposition_type
    from source
)

select * from renamed;

