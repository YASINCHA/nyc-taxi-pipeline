-- ===========================================================================
-- trip_efficiency.sql — Gold : efficacité des courses par jour
-- Utilise la macro revenue_per_mile
-- ===========================================================================

SELECT
    trip_date,
    trip_count,
    revenue,
    total_distance,
    {{ revenue_per_mile('revenue', 'total_distance') }} AS avg_revenue_per_mile,
    avg_revenue_per_trip,
    avg_distance_per_trip
FROM {{ ref('daily_taxi_summary') }}
ORDER BY trip_date