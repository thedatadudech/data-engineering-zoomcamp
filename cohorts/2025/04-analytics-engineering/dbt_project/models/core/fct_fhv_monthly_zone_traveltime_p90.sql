{{ config(
    materialized = "table"
) }}

-- This model calculates continuous P90 travel time for FHV trips
-- partitioned by year, month, pickup_location_id, and dropoff_location_id,
-- including zone lookup for location IDs.

WITH valid_fhv_trips AS (
    SELECT
        fhv.pickup_datetime,
        fhv.dropoff_datetime,
        EXTRACT(YEAR FROM fhv.pickup_datetime) AS year,
        EXTRACT(MONTH FROM fhv.pickup_datetime) AS month,
        fhv.pickup_location_id,
        pickup_zone.zone AS pickup_zone,
        fhv.dropoff_location_id,
        dropoff_zone.zone AS dropoff_zone,
        TIMESTAMP_DIFF(fhv.dropoff_datetime, fhv.pickup_datetime, SECOND) AS trip_duration
      FROM {{ ref('dim_fhv_tripdata') }} fhv
   LEFT JOIN {{ ref('dim_zones') }} pickup_zone
        ON CAST(fhv.pickup_location_id AS INT64) = pickup_zone.locationid
    LEFT JOIN {{ ref('dim_zones') }} dropoff_zone
        ON CAST(fhv.dropoff_location_id AS INT64) = dropoff_zone.locationid
    WHERE fhv.dispatching_base_num IS NOT NULL
),

trip_duration_p90 AS (
    SELECT distinct
        year,
        month,
        pickup_location_id,
        pickup_zone,
        dropoff_location_id,
        dropoff_zone,
        PERCENTILE_CONT(trip_duration, 0.90) OVER
            (PARTITION BY pickup_location_id, dropoff_location_id, year, month
            
        ) AS p90_trip_duration,

    FROM valid_fhv_trips
    WHERE pickup_zone in ('Newark Airport', 'SoHo','Yorkville East')
    AND year = 2019
    AND month =11
),

ranked as ( SELECT DISTINCT 
    year, 
    month, 
    pickup_location_id, 
    pickup_zone, 
    dropoff_location_id, 
    dropoff_zone, 
    p90_trip_duration,
    RANK() OVER (
            PARTITION BY pickup_zone, year, month  ORDER BY pickup_zone, year, month, p90_trip_duration DESC
        ) AS rank
FROM trip_duration_p90 
)
select * from ranked
where rank=2
order by year, month, pickup_zone, rank


