{{ config(materialized='table') }}

with date_gen as
(
select
    row_number() over(order by seq4()) -1 as rn,
    to_date(dateadd(day, rn, to_date('2023-01-01'))) as date,
    {{ generate_date_key(date) }} as date_id as date_id
from
     table(generator(rowcount => 730))
)
select
    date_id,
    date,
    extract(dayofweek from date) as day_of_week,
    extract(day from date) as day_of_month,
    extract(week from date) as week_of_year,
    extract(month from date) as month,
    extract(year from date) as year,
    case when extract( dayofweek from date) in (6,0) then true
        else false end as weekend_flag
from date_gen