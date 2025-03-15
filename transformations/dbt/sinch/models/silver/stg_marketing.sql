{{ 
    config(
        materialized='table'
    ) 
}}

WITH deduplicated_campaigns AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY campaignid ORDER BY campaignstartdate DESC) AS row_num
    FROM 
        {{ ref('base_marketing') }}
    WHERE 
        campaignid IS NOT NULL
)

SELECT 
    campaignid,
    targetaudience,
    campaignstartdate,
    campaignenddate,
    source_name,
    CURRENT_TIMESTAMP as insert_date
FROM 
    deduplicated_campaigns
WHERE 
    row_num = 1
