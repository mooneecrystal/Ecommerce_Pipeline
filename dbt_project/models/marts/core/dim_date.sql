{{ config(
    materialized='table',
    tags=['gold', 'core']
) }}

{% set fiscal_year_start_month = 6 %}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="dateadd(year, 2, current_date())"
    ) }}

),

calculations as (
    select
        date_day as date_actual,
        cast(to_char(date_day, 'YYYYMMDD') as int) as date_key,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(quarter from date_day) as quarter,
        
        -- Fiscal Year Logic
        case 
            when extract(month from date_day) >= {{ fiscal_year_start_month }} 
            then extract(year from date_day) + 1 
            else extract(year from date_day) 
        end as fiscal_year,

        -- Fiscal Quarter Logic
        floor(((extract(month from date_day) + (12 - {{ fiscal_year_start_month }} + 1) - 1) % 12) / 3) + 1 as fiscal_quarter
    from date_spine
),

final_dates as (
    select
        date_key,
        date_actual,
        year,
        quarter,
        month,
        fiscal_year,
        fiscal_quarter,
        to_char(date_actual, 'MMMM') as month_name,
        to_char(date_actual, 'Day') as day_name,
        case when extract(dayofweek from date_actual) in (0, 6) then true else false end as is_weekend
    from calculations
)

-- The Union: Add the 'Unknown' row for Fact joins
select * from final_dates

-- Add default -1 for Unknown date.
union all

select
    -1 as date_key,
    cast('1900-01-01' as date) as date_actual,
    0 as year,
    0 as quarter,
    0 as month,
    0 as fiscal_year,
    0 as fiscal_quarter,
    'MISSING' as month_name,
    'MISSING' as day_name,
    false as is_weekend