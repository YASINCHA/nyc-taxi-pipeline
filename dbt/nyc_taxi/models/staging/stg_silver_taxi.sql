-- ===========================================================================
-- stg_silver_taxi.sql — Staging : lecture de la table Silver
-- Source : final_taxi_data (chargée par Spark load.py)
-- ===========================================================================

SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    total_amount,
    fare_amount,
    tip_amount,
    tolls_amount,
    "PULocationID"  AS pickup_location_id,
    "DOLocationID"  AS dropoff_location_id,
    DATE(tpep_pickup_datetime) AS trip_date
FROM {{ source('public', 'final_taxi_data') }}
WHERE tpep_pickup_datetime IS NOT NULL
  AND total_amount > 0
  AND trip_distance > 0