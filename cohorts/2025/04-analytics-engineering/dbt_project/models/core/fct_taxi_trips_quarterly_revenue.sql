{{ config(
    materialized='table'
) }}

WITH trip_data AS (
    SELECT 
        service_type,
        DATE_TRUNC(pickup_datetime, QUARTER) AS quarter_start,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        CONCAT(EXTRACT(YEAR FROM pickup_datetime), '/Q', EXTRACT(QUARTER FROM pickup_datetime)) AS year_quarter,
        SUM(total_amount) AS total_revenue
    FROM {{ ref('fact_trips') }}
    GROUP BY service_type, quarter_start, year, quarter, year_quarter
),
quarterly_revenue_growth AS (
    SELECT 
        t1.service_type,
        t1.year_quarter,
        t1.total_revenue,
        t2.total_revenue AS last_year_revenue,
        ROUND((t1.total_revenue - t2.total_revenue) / t2.total_revenue * 100, 2) AS yoy_growth
    FROM trip_data t1
    LEFT JOIN trip_data t2
        ON t1.service_type = t2.service_type
        AND t1.year = t2.year + 1
        AND t1.quarter = t2.quarter
)
SELECT * FROM quarterly_revenue_growth
