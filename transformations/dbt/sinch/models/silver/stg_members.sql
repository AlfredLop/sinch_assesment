{{ 
    config(
        materialized='table'
    ) 
}}

WITH deduplicated_members AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY memberid ORDER BY joindate DESC) AS row_num
    FROM 
        {{ ref('base_members') }}
    WHERE 
        memberid IS NOT NULL
)

SELECT 
    memberid,
    membername,
    membershiptype,
    joindate,
    expirationdate,
    source_name,
    CURRENT_TIMESTAMP as insert_date
FROM 
    deduplicated_members
WHERE 
    row_num = 1
