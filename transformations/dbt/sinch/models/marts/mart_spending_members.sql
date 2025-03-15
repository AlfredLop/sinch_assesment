{{ 
    config(
        materialized='table'
    ) 
}}

WITH monthly_spending AS (
    SELECT 
        DATE_TRUNC('month', orderdate) AS month, 
        membername, 
        SUM(total) AS total_spent
    FROM 
        {{ ref('build_orders') }}
    WHERE 
        membername IS NOT NULL
    GROUP BY 
        month, 
        membername

), ranked_spending AS (
    -- using rank to handle ties and not just pick 1 arbitrarily using row_number
    SELECT 
        *, 
        RANK() OVER (PARTITION BY month ORDER BY total_spent DESC) AS highest_rank,
        RANK() OVER (PARTITION BY month ORDER BY total_spent ASC) AS lowest_rank
    FROM 
        monthly_spending

), labeled_spending AS (
    SELECT 
        month,
        membername,
        total_spent,
        CASE 
            WHEN highest_rank = 1 THEN 'Highest Spending'
            WHEN lowest_rank = 1 THEN 'Lowest Spending'
            ELSE NULL 
        END AS spending_label
    FROM 
        ranked_spending
    WHERE 
        highest_rank <= 1 
        OR lowest_rank <= 1
)

SELECT 
    * 
FROM 
    labeled_spending 
WHERE 
    spending_label IS NOT NULL
