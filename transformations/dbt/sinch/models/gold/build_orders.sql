{{ 
    config(
        materialized='table'
    ) 
}}

WITH enriched_orders AS (
    SELECT 
        o.orderid,
        o.orderdate,
        o.subtotal,
        o.total,
        {{ dbt_utils.star(ref('stg_members'), except=["memberid", "source_name", "insert_date"]) }},
        {{ dbt_utils.star(ref('stg_marketing'), except=["campaignid", "source_name", "insert_date"]) }},
        CURRENT_TIMESTAMP AS insert_date
    FROM 
        {{ ref('stg_orders') }} o
    LEFT JOIN 
        {{ ref('stg_members') }} m ON o.memberid = m.memberid
    LEFT JOIN 
        {{ ref('stg_marketing') }} mk ON o.campaignid = mk.campaignid
)

SELECT 
    * 
FROM enriched_orders
