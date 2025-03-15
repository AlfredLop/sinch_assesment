{% macro test_expect_column_values_to_be_positive(model, column_name) %}
WITH validation AS (
    SELECT 
        {{ column_name }} 
    FROM {{ model }}
    WHERE {{ column_name }} < 0
)
SELECT * FROM validation
{% endmacro %}


{% macro test_expect_column_values_to_be_after(model, column_name, min_column_name) %}
WITH validation AS (
    SELECT 
        {{ column_name }}, 
        {{ min_column_name }} 
    FROM {{ model }}
    WHERE {{ column_name }} < CAST({{ min_column_name }} AS DATE)
)
SELECT * FROM validation
{% endmacro %}
