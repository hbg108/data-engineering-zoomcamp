{{ config(materialized='table') }}

with trip_data as (
    select 
    -- Revenue grouping 
    service_type, 
    FORMAT_TIMESTAMP("%Y/Q%Q", pickup_datetime) as year_quarter, 

    count(tripid) as num_trips,
    -- Revenue calculation 
    sum(total_amount) as revenue

    from {{ ref('fact_trips') }}
    group by 1,2
)

select 
d1.*,
round(((d1.revenue - d2.revenue) / d2.revenue) * 100, 2) growth
from trip_data d1
left join trip_data d2
on d1.service_type = d2.service_type
and cast(split(d1.year_quarter, '/')[0] as int64) - 1 = cast(split(d2.year_quarter, '/')[0] as int64)
and split(d1.year_quarter, '/')[1] = split(d2.year_quarter, '/')[1]
order by service_type ASC, year_quarter DESC