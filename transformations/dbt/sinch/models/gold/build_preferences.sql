{{ 
    config(
        materialized='table'
    ) 
}}
WITH preferences_cleaned AS (
    SELECT 
        {{ dbt_utils.star(ref('stg_preferences'), except=["insert_date"]) }},
        CURRENT_TIMESTAMP AS insert_date
    FROM 
        {{ ref('stg_preferences') }}
)

SELECT 
    * 
FROM 
    preferences_cleaned
