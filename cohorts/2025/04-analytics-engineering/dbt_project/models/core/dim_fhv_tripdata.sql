{{ config(
    materialized='table'
) }}

with fhv_tripdata as (
    select * 
    from {{ ref('stg_fhv_tripdata') }}
    where dispatching_base_num is not null
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    EXTRACT(YEAR FROM fhv_tripdata.pickup_datetime) AS year,
    EXTRACT(MONTH FROM fhv_tripdata.pickup_datetime) AS month,
    CAST(fhv_tripdata.pickup_location_id AS INT64) AS pickup_location_id, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    CAST(fhv_tripdata.dropoff_location_id AS INT64) AS dropoff_location_id,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone
from fhv_tripdata
inner join dim_zones as pickup_zone
on CAST(fhv_tripdata.pickup_location_id AS INT64) = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on CAST(fhv_tripdata.dropoff_location_id AS INT64) = dropoff_zone.locationid

