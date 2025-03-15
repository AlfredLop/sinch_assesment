{{ 
    config(
        materialized='table'
    ) 
}}

WITH filtered_items AS (
    SELECT 
        *
    FROM 
        {{ ref('base_orders_items') }}
    WHERE 
        orderid IS NOT NULL AND itemname IS NOT NULL
)

SELECT 
    orderid,
    itemname,
    price,
    source_name,
    CURRENT_TIMESTAMP as insert_date
FROM 
    filtered_items

