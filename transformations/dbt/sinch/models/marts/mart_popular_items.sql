{{ 
    config(
        materialized='table'
    ) 
}}

WITH item_popularity AS (
    SELECT 
        TO_CHAR(DATE_TRUNC('month', o.orderdate), 'Mon-YYYY') AS month_year,
        oi.itemname,
        COUNT(*) AS order_count
    FROM 
        {{ ref('build_orders') }} o
    JOIN 
        {{ ref('build_order_items') }} oi ON o.orderid = oi.orderid
    GROUP BY 
        month_year, 
        oi.itemname
), ranked_items AS (
    -- using rank to handle ties and not just pick 1 arbitrarily using row_number
    SELECT 
        *, 
        RANK() OVER (PARTITION BY month_year ORDER BY order_count DESC) AS most_popular_rank,
        RANK() OVER (PARTITION BY month_year ORDER BY order_count ASC) AS least_popular_rank
    FROM 
        item_popularity
), labeled_items AS (
    SELECT 
        month_year,
        itemname,
        order_count,
        CASE 
            WHEN most_popular_rank = 1 THEN 'Most Popular'
            WHEN most_popular_rank = 2 THEN 'Second Most Popular'
            WHEN least_popular_rank = 1 THEN 'Least Popular'
            WHEN least_popular_rank = 2 THEN 'Second Least Popular'
            ELSE NULL 
        END AS popularity_label
    FROM 
        ranked_items
    WHERE 
        most_popular_rank <= 2 
        OR least_popular_rank <= 2
)

SELECT 
    * 
FROM 
    labeled_items 