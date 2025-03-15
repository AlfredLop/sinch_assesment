{{ 
    config(
        materialized='table'
    ) 
}}

WITH order_items_cleaned AS (
    SELECT 
        {{ dbt_utils.star(ref('stg_orders_items'), except=["insert_date"]) }},
        CURRENT_TIMESTAMP AS insert_date
    FROM 
        {{ ref('stg_orders_items') }}
)

SELECT * FROM order_items_cleaned
