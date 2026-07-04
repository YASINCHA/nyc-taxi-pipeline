-- ===========================================================================
-- daily_taxi_summary.sql — Gold : résumé quotidien des courses
-- Remplace l'agrégation PySpark dans load.py
-- ===========================================================================

SELECT
    trip_date,
    COUNT(*)                        AS trip_count,
    ROUND(SUM(total_amount)::numeric, 2)   AS revenue,
    ROUND(SUM(trip_distance)::numeric, 2)  AS total_distance,
    ROUND(AVG(total_amount)::numeric, 2)   AS avg_revenue_per_trip,
    ROUND(AVG(trip_distance)::numeric, 2)  AS avg_distance_per_trip,
    ROUND(AVG(passenger_count)::numeric, 2) AS avg_passengers
FROM {{ ref('stg_silver_taxi') }}
GROUP BY trip_date
ORDER BY trip_date