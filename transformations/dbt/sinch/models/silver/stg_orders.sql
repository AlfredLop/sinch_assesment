{{ 
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='orderid'
    ) 
}}

WITH deduplicated_orders AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY orderid ORDER BY orderdate DESC) AS row_num
    FROM 
        {{ ref('base_orders') }}
    {% if is_incremental() %}
    -- Process only new or updated records from the last orderdate, including the last order date records
    WHERE orderdate >= (SELECT MAX(orderdate) FROM {{ this }})
    {% endif %}
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