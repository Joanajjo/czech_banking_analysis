With source as (
    select
        *
    from `czech-banking-analysis`.`raw`.`transactions`
),

renamed as ( 
    select
        trans_id as transaction_id,
        account_id,
        parse_date('%y%m%d', cast(date as string)) as transaction_date,
        type as transaction_type,
        operation as operation_type,
        amount as transaction_amount,
        balance as balance_after_transaction,
        k_symbol as transaction_category,
        bank as counterpart_bank,
        account as counterpart_account
    from source

)

select * from renamed