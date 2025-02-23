{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('dim_fhv_trips') }}
)

-- select *,
--     TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) trip_duration,
--     percentile_cont(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND), 0.9) over(partition by year, month, pickup_location_id, dropoff_location_id) p90
-- from trips_data
-- ORDER BY year, month, pickup_datetime

select DISTINCT year,
    month,
    pickup_location_id,
    pickup_zone,
    dropoff_location_id,
    dropoff_zone,
    percentile_cont(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND), 0.9) over(partition by year, month, pickup_location_id, dropoff_location_id) p90
from trips_data
ORDER BY year, month, p90 DESC
