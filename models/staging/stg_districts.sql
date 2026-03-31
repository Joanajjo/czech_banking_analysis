with source as (
    select * from {{source('raw','districts')}}
),
renamed as (
    SELECT
        a1 as district_id,
        a2 as district_name,
        a3 as region,
        a4 as number_of_residents,
        a5 as number_of_municipalities_with_less_than_499_residents,
        a6 as number_of_municipalities_with_500_to_1999_residents,
        a7 as number_of_municipalities_with_2000_to_9999_residents,
        a8 as number_of_municipalities_with_10000_or_more_residents,
        a9 as number_of_cities,
        a10 as ratio_of_resitends_to_cities,
        a11 as avg_salary,
        a12 as unemployment_rate_95,
        a13 as unemployment_rate_96,
        a15 as crime_rate_95
    from source

)
select * from renamed