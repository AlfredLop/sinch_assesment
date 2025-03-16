{{ 
    config(
        materialized='table'
    ) 
}}
WITH preferences_cleaned AS (
    SELECT 
        {{ dbt_utils.star(ref('stg_members'), except=["insert_date","memberid","source_name"]) }},
        CURRENT_TIMESTAMP AS insert_date
    FROM 
        {{ ref('stg_preferences') }} p
    LEFT JOIN
        {{ ref('stg_members') }} m
    ON
        p.memberid = m.memberid
)

SELECT 
    * 
FROM 
    preferences_cleaned
