{{ 
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='orderid'
    ) 
}}

WITH new_data AS (
    SELECT 
        orderid::VARCHAR(100) AS orderid,
        memberid::VARCHAR(100) AS memberid,
        storeid::VARCHAR(100) AS storeid,
        campaignid::VARCHAR(100) AS campaignid,
        orderdate,
        subtotal::DECIMAL(10,2) AS subtotal,
        total::DECIMAL(10,2) AS total,
        source_name,
        CURRENT_TIMESTAMP AS insert_date
    FROM {{ source('public', 'orders_raw') }}
    
    {% if is_incremental() %}
    -- Process only new or updated records from the last orderdate, including the last order date records
    WHERE orderdate >= (SELECT MAX(orderdate) FROM {{ this }})
    {% endif %}
)

SELECT * FROM new_data
