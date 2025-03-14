{{ 
    config(
        materialized='table'
    ) 
}}

SELECT 
    orderid::VARCHAR(100),
    status::VARCHAR(50),
    TO_TIMESTAMP(statustimestamp, 'DD/MM/YYYY HH24:MI:SS') AS statustimestamp, 
    source_name,
    CURRENT_TIMESTAMP AS insert_date
FROM 
    {{ source('public', 'order_status_raw') }}
WHERE 
    statustimestamp IS NOT NULL
    