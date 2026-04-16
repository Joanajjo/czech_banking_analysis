with source as (
    select * from {{source('raw', 'dispositions')}}
),
renamed as (
    select 
        disp_id as disposition_id,
        client_id,
        account_id,
        type as disposition_type
    from source
)

select * from renamed