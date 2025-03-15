{{ 
    config(
        materialized='table'
    ) 
}}

WITH order_status_cleaned AS (
    SELECT 
        {{ dbt_utils.star(ref('stg_order_status'), except=["insert_date"]) }},
        CURRENT_TIMESTAMP AS insert_date
    FROM 
        {{ ref('stg_order_status') }}
)

SELECT 
    * 
FROM order_status_cleaned
