{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
    where fare_amount > 0
    and trip_distance > 0
    and payment_type_description in ('Cash', 'Credit card')
)

select DISTINCT service_type, 
    FORMAT_TIMESTAMP("%Y", pickup_datetime) year,
    FORMAT_TIMESTAMP("%m", pickup_datetime) month,
    percentile_cont(fare_amount, 0.97) over(partition by service_type, FORMAT_TIMESTAMP("%Y", pickup_datetime), FORMAT_TIMESTAMP("%m", pickup_datetime)) p97,
    percentile_cont(fare_amount, 0.95) over(partition by service_type, FORMAT_TIMESTAMP("%Y", pickup_datetime), FORMAT_TIMESTAMP("%m", pickup_datetime)) p95,
    percentile_cont(fare_amount, 0.9) over(partition by service_type, FORMAT_TIMESTAMP("%Y", pickup_datetime), FORMAT_TIMESTAMP("%m", pickup_datetime)) p90
from trips_data
ORDER BY service_type, year, month
