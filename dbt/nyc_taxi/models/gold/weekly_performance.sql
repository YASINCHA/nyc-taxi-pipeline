-- ===========================================================================
-- weekly_performance.sql — Gold : performance par jour de la semaine
-- Remplace la vue v_weekly_performance dans init_schema.sql
-- ===========================================================================

SELECT
    TO_CHAR(trip_date, 'Day')            AS day_of_week,
    EXTRACT(DOW FROM trip_date)          AS dow_number,
    ROUND(AVG(revenue)::numeric, 2)      AS avg_daily_revenue,
    ROUND(AVG(trip_count)::numeric, 0)   AS avg_daily_trips,
    ROUND(AVG(total_distance)::numeric, 2) AS avg_daily_distance,
    COUNT(*)                             AS data_points
FROM {{ ref('daily_taxi_summary') }}
GROUP BY TO_CHAR(trip_date, 'Day'), EXTRACT(DOW FROM trip_date)
ORDER BY dow_number