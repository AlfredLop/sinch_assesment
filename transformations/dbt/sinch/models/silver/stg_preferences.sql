{{ 
    config(
        materialized='table'
    ) 
}}

WITH filtered_preferences AS (
    SELECT 
        *
    FROM 
        {{ ref('base_preferences') }}
    WHERE 
        memberid IS NOT NULL AND preference IS NOT NULL
)

SELECT 
    memberid,
    preference,
    source_name,
    CURRENT_TIMESTAMP as insert_date
FROM 
    filtered_preferences
