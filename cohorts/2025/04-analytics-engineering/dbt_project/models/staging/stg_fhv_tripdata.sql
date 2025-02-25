{{ config(
    materialized='view'
) }}

-- Staging model for FHV trip data
-- Filters out records with null dispatching_base_num

with raw_fhv_trips as (
    select *
    from {{ source('staging', 'fhv_tripdata') }}
    where dispatching_base_num is not null
)

select 
    unique_row_id,
    filename,
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    PUlocationID as pickup_location_id,
    DOlocationID as dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
from raw_fhv_trips