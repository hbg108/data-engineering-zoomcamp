{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
)
    select 
    -- Revenue grouping 
    FORMAT_TIMESTAMP("%Y/Q%Q", pickup_datetime) as revenue_quarter, 

    service_type, 

    -- Revenue calculation 
    sum(total_amount) as revenue_quarterly_total_amount

    from trips_data
    group by 1,2