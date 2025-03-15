{{ 
    config(
        materialized='table'
    ) 
}}

SELECT 
    campaignid:: VARCHAR(100) AS campaignid,
    targetaudience:: TEXT AS targetaudience,
    TO_DATE(campaignstartdate, 'DD/MM/YYYY') AS campaignstartdate,
    TO_DATE(campaignenddate, 'DD/MM/YYYY') AS campaignenddate,
    source_name,
    CURRENT_TIMESTAMP AS insert_date
FROM 
    {{ source('public', 'marketing_raw') }}
