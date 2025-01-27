-- Step 1: Create the trips table
CREATE TABLE IF NOT EXISTS trips (
    VendorID INT,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag CHAR(1),
    RatecodeID INT,
    PULocationID INT,
    DOLocationID INT,
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    ehail_fee FLOAT,  -- Added missing column
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    payment_type INT,
    trip_type INT,
    congestion_surcharge FLOAT
);

-- Step 2: Truncate the table (if needed, e.g., for reloading data)
TRUNCATE TABLE trips;

-- Step 3: Load the data into the trips table
COPY trips(VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag,
           RatecodeID, PULocationID, DOLocationID, passenger_count, trip_distance,
           fare_amount, extra, mta_tax, tip_amount, tolls_amount, 
           ehail_fee, improvement_surcharge, total_amount, payment_type, trip_type, 
           congestion_surcharge)
FROM '/var/lib/postgresql/data/green_tripdata_2019-10.csv'
DELIMITER ','
CSV HEADER;

-- Step 4: Verify the data load
SELECT COUNT(*) AS total_rows FROM trips;
SELECT * FROM trips LIMIT 10;

-- Step 5: Additional queries to analyze the data
-- Question 3: Trip segmentation count
SELECT 
    COUNT(*) FILTER (WHERE trip_distance <= 1) AS "Up to 1 mile",
    COUNT(*) FILTER (WHERE trip_distance > 1 AND trip_distance <= 3) AS "1 to 3 miles",
    COUNT(*) FILTER (WHERE trip_distance > 3 AND trip_distance <= 7) AS "3 to 7 miles",
    COUNT(*) FILTER (WHERE trip_distance > 7 AND trip_distance <= 10) AS "7 to 10 miles",
    COUNT(*) FILTER (WHERE trip_distance > 10) AS "Over 10 miles"
FROM trips
WHERE lpep_pickup_datetime >= '2019-10-01'
  AND lpep_pickup_datetime < '2019-11-01';

-- Question 4: Longest trip for each day
SELECT 
    DATE(lpep_pickup_datetime) AS pickup_day,
    MAX(trip_distance) AS longest_trip
FROM trips
WHERE lpep_pickup_datetime >= '2019-10-01'
  AND lpep_pickup_datetime < '2019-11-01'
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY longest_trip DESC
LIMIT 1;

-- Question 5: Three biggest pickup zones
SELECT 
    zones.zone AS pickup_zone,
    SUM(t.total_amount) AS total_amount
FROM trips t
JOIN taxi_zone_lookup zones ON t.PULocationID = zones.LocationID
WHERE DATE(t.lpep_pickup_datetime) = '2019-10-18'
GROUP BY zones.zone
HAVING SUM(t.total_amount) > 13000
ORDER BY total_amount DESC;

-- Question 6: Largest tip
SELECT 
    dz.zone AS dropoff_zone,
    MAX(t.tip_amount) AS largest_tip
FROM trips t
JOIN taxi_zone_lookup pz ON t.PULocationID = pz.LocationID
JOIN taxi_zone_lookup dz ON t.DOLocationID = dz.LocationID
WHERE pz.zone = 'East Harlem North'
  AND t.lpep_pickup_datetime >= '2019-10-01'
  AND t.lpep_pickup_datetime < '2019-11-01'
GROUP BY dz.zone
ORDER BY largest_tip DESC
LIMIT 1;
