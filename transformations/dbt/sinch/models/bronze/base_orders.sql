{{ 
    config(
        materialized='table'
    ) 
}}


SELECT 
    orderid::VARCHAR(100) AS orderid,
    memberid::VARCHAR(100) AS memberid,
    storeid::VARCHAR(100) AS storeid,
    campaignid::VARCHAR(100) AS campaignid,
    TO_DATE(orderdate, 'DD/MM/YYYY') AS orderdate,
    subtotal::DECIMAL(10,2) AS subtotal,
    total::DECIMAL(10,2) AS total,
    source_name,
    CURRENT_TIMESTAMP AS insert_date
FROM 
    {{ source('public', 'orders_raw') }}
