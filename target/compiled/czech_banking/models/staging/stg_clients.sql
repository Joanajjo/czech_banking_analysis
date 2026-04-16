with source as (
    select * from `czech-banking-analysis`.`raw`.`clients`
),

renamed as (
    select
        client_id,
        district_id,
        case
            when cast(substr(cast(birth_number as string), 3, 2) as int64) > 50
            then 'F' else 'M'
        end                                    as gender,
        date(
            1900 + cast(substr(cast(birth_number as string), 1, 2) as int64),
            case when cast(substr(cast(birth_number as string), 3, 2) as int64) > 50
                 then cast(substr(cast(birth_number as string), 3, 2) as int64) - 50
                 else cast(substr(cast(birth_number as string), 3, 2) as int64) end,
            cast(substr(cast(birth_number as string), 5, 2) as int64)
        )                                      as date_of_birth
    from source
)

select * from renamed