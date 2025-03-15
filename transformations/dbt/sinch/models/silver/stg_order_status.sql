{{ 
    config(
        materialized='table'
    ) 
}}

WITH deduplicated_status AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY orderid, status ORDER BY statustimestamp DESC) AS row_num
    FROM 
        {{ ref('base_order_status') }}
    WHERE 
        orderid IS NOT NULL AND statustimestamp IS NOT NULL
)

SELECT 
    orderid,
    status,
    statustimestamp,
    source_name,
    CURRENT_TIMESTAMP as insert_date
FROM 
    deduplicated_status
WHERE 
    row_num = 1
