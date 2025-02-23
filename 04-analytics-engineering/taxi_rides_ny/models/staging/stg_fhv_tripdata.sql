{{
    config(
        materialized='view'
    )
}}

select 
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime dropoff_datetime,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_location_id,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_location_id,
    sr_flag,
    affiliated_base_number
from {{ source('staging','fhv_tripdata') }}
where dispatching_base_num is not null