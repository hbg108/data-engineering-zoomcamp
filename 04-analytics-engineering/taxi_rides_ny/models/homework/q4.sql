{{ config(
    schema=resolve_schema_for('stg')
) }}

select * from {{ ref('dim_fhv_trips') }}