{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select FORMAT_TIMESTAMP("%Y", fhv_tripdata.pickup_datetime) year,
    FORMAT_TIMESTAMP("%m", fhv_tripdata.pickup_datetime) month,
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.pickup_location_id,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_tripdata.dropoff_location_id,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_tripdata.sr_flag,
    fhv_tripdata.affiliated_base_number
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_location_id = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_location_id = dropoff_zone.locationid