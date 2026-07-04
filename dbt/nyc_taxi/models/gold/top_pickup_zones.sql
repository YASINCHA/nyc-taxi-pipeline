-- ===========================================================================
-- top_pickup_zones.sql — Gold : Top 20 zones de pickup les plus rentables
-- Insight métier : quelles zones génèrent le plus de revenus ?
-- Source : stg_silver_taxi (via final_taxi_data)
-- ===========================================================================

SELECT
    pickup_location_id,
    COUNT(*)                                    AS trip_count,
    ROUND(SUM(total_amount)::numeric, 2)        AS total_revenue,
    ROUND(AVG(total_amount)::numeric, 2)        AS avg_revenue_per_trip,
    ROUND(AVG(trip_distance)::numeric, 2)       AS avg_distance_miles,
    ROUND(AVG(passenger_count)::numeric, 2)     AS avg_passengers,
    ROUND(SUM(tip_amount)::numeric, 2)          AS total_tips,
    ROUND(AVG(tip_amount)::numeric, 2)          AS avg_tip_per_trip
FROM {{ ref('stg_silver_taxi') }}
WHERE pickup_location_id IS NOT NULL
GROUP BY pickup_location_id
ORDER BY total_revenue DESC
LIMIT 20