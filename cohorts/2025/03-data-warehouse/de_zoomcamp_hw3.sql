

--Question 1
select count(*)  FROM `eternal-tendril-450412-f1.de_zoomcamp.external_yellow_tripdata`;


--Question 2
SELECT distinct count(PULocationID) FROM `eternal-tendril-450412-f1.de_zoomcamp.yellow_tripdata` ;
SELECT distinct count(PULocationID) FROM `eternal-tendril-450412-f1.de_zoomcamp.external_yellow_tripdata` ;


--Question 3
SELECT PULocationID, DOLocationID FROM `eternal-tendril-450412-f1.de_zoomcamp.yellow_tripdata` ;

--Question 4
SELECT count(*) from  `de_zoomcamp.yellow_tripdata` where fare_amount = 0;

--Question 5
CREATE OR REPLACE TABLE de_zoomcamp.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM de_zoomcamp.yellow_tripdata;


--Question 6
select distinct(VendorID) from `de_zoomcamp.yellow_tripdata_partitoned_clustered` where tpep_dropoff_datetime between '2024-03-01' and '2024-03-15'