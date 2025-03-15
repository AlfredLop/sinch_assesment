{{ 
    config(
        materialized='table'
    ) 
}}

WITH revenue_summary AS (
    SELECT 
        storeid, 
        campaignid, 
        SUM(total) AS total_revenue
    FROM 
        {{ ref('build_orders') }}
    GROUP BY 
        storeid, campaignid
)

SELECT 
    * 
FROM 
    revenue_summary
