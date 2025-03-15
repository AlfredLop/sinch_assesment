{{ 
    config(
        materialized='table'
    ) 
}}

SELECT 
    id:: VARCHAR(100) AS memberid,
    name:: TEXT AS membername,
    membershiptype:: VARCHAR(50) AS membershiptype,
    TO_DATE(joindate, 'DD/MM/YYYY') AS joindate,
    TO_DATE(expirationdate, 'DD/MM/YYYY') AS expirationdate,
    source_name,
    CURRENT_TIMESTAMP AS insert_date
FROM 
    {{ source('public', 'members_raw') }}
