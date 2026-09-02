-- models/gold_LCBO_marts/gold_dim_date.sql
{{ 
    config(
        materialized='table', 
        target_schema='gold'
        ) 
}}

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-01-01' as date)"
    ) }}

),

dates as (

    select
        cast(date_day as date) as date_day
    from date_spine

)

select
    -- surrogate key as an integer YYYYMMDD (common for date dims)
    cast(to_char(date_day, 'YYYYMMDD') as integer)  as date_sk,
    date_day,
    extract(year    from date_day)                  as year,
    extract(quarter from date_day)                  as quarter,
    extract(month   from date_day)                  as month,
    to_char(date_day, 'Month')                      as month_name,
    extract(day     from date_day)                  as day_of_month,
    extract(dow     from date_day)                  as day_of_week,   -- 0=Sunday
    to_char(date_day, 'Day')                        as day_name,
    case when extract(dow from date_day) in (0,6)
         then true else false end                   as is_weekend
from dates