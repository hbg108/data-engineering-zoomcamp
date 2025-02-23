-- select *
-- from {{ ref('fact_trips') }}
-- where pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", 30) }}' DAY

-- select *
-- from {{ ref('fact_trips') }}
-- where pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DBT_DAYS_BACK", "30") }}' DAY

select *
from {{ ref('fact_trips') }}
where pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DBT_DAYS_BACK", "30")) }}' DAY

-- select *
-- from {{ ref('fact_trips') }}
-- where pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DBT_DAYS_BACK", var("days_back", "30")) }}' DAY