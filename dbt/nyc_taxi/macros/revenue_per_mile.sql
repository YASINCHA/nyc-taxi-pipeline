{% macro revenue_per_mile(revenue_col, distance_col) %}
    CASE
        WHEN {{ distance_col }} > 0
        THEN ROUND(({{ revenue_col }} / {{ distance_col }})::numeric, 2)
        ELSE 0
    END
{% endmacro %}