{{ 
    config(
        materialized='table'
    ) 
}}


SELECT 
    memberid:: VARCHAR(100) AS memberid,
    preference:: TEXT AS preference,
    source_name,
    CURRENT_TIMESTAMP AS insert_date
FROM 
    {{ source('public', 'preferences_raw') }}
