{{ 
    config(
        materialized='table'
    ) 
}}

WITH processing_times AS (
    SELECT 
        o.storeid, 
        o.orderid,
        MIN(os.statustimestamp) FILTER (WHERE os.status = 'Submitted') AS submitted_time,
        MIN(os.statustimestamp) FILTER (WHERE os.status = 'Delivered') AS delivered_time
    FROM 
        {{ ref('build_orders') }} o
    JOIN 
        {{ ref('build_order_status') }} os ON o.orderid = os.orderid
    GROUP BY 
        o.storeid, 
        o.orderid

), avg_times AS (
    SELECT 
        storeid,
        AVG(delivered_time - submitted_time) AS avg_delivery_time
    FROM 
        processing_times
    WHERE 
        delivered_time IS NOT NULL AND submitted_time IS NOT NULL
    GROUP BY 
        storeid
)
SELECT 
    storeid, 
    avg_delivery_time 
FROM avg_times
