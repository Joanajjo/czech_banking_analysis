-- Answers Q3: Which districts have the highest default rates and does local unemployment explain it?
-- Granularity: one row per district

with district_benchmarks as (
    select
        -- your dimensions here
        district_name,
        region,
        count(loan_id) as total_loans,
        countif(is_default_risk = true) as total_defaults,
        round(countif(is_default_risk = true) * 100.0 / count(loan_id), 1) as default_rate_pct,
        -- Economic indicators to answer: Does local economy explain default rates?
        round(avg(safe_cast(unemployment_rate_95 as float64)), 2) as avg_unemployment_95,
        round(avg(safe_cast(crime_rate_95 as float64)), 2) as avg_crime_rate_95,
        round(avg(safe_cast(avg_salary as float64)), 0) as avg_salary


    from {{ ref('fct_loans') }}
    group by district_name, region
)

select * from district_benchmarks