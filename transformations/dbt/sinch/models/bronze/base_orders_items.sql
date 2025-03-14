{{ 
    config(
        materialized='table'
    ) 
}}

SELECT 
    orderid:: VARCHAR(100) AS orderid,
    itemname:: TEXT AS itemname,
    price:: DECIMAL(10,2) AS price,
    source_name,
    CURRENT_TIMESTAMP AS insert_date
FROM 
    {{ source('public', 'orders_items_raw') }}
