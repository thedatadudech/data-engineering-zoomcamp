{{ config(
    materialized = "table"
) }}

-- This model calculates continuous percentiles (P90, P95, P97)
-- using DISTINCT fare_amount partitioned by service_type, year, and month.
-- Instead of window functions, we use an aggregation approach.

WITH valid_trips AS (
    SELECT
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        fare_amount
    FROM {{ ref('fact_trips') }}
    WHERE fare_amount > 0
      AND trip_distance > 0
      AND payment_type_description IN ('Cash', 'Credit Card')
),

distinct_fares AS (
    SELECT DISTINCT service_type, year, month, fare_amount 
    FROM valid_trips
),

percentiles AS (
    SELECT
        service_type,
        year,
        month,
        PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS fare_p90,
        PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS fare_p95,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS fare_p97
    FROM distinct_fares
)

SELECT DISTINCT service_type, year, month, fare_p90, fare_p95, fare_p97 
FROM percentiles


