with source as (
    Select *
    from {{source('raw','accounts')}}
),
renamed as (
    select
        account_id,
        district_id,
        frequency,
        parse_date('%y%m%d', cast(date as string)) as account_created_on
        from source
)
select * from renamed