{{ config(materialized='table') }}

WITH deduplicated_orders AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY orderid ORDER BY orderdate DESC) AS row_num
    FROM 
        {{ ref('base_orders') }}
    WHERE 
        orderid IS NOT NULL
)

SELECT 
    orderid,
    memberid,
    storeid,
    campaignid,
    orderdate,
    subtotal,
    total,
    source_name,
    CURRENT_TIMESTAMP as insert_date
FROM 
    deduplicated_orders
WHERE 
    row_num = 1