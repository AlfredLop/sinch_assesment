{{ 
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='orderid'
    ) 
}}

WITH  base_filtered as (
    SELECT
        *
    FROM 
        {{ ref('stg_orders') }}
    {% if is_incremental() %}
    -- Process only new or updated records from the last orderdate, including the last order date records
    WHERE orderdate >= (SELECT MAX(orderdate) FROM {{ this }})
    {% endif %}
)
, enriched_orders AS (
    -- This select using 2 components
        -- get_filtered_columns_in_relation: this is to dinamically create a list of columns for the select while excluding some
        -- jinja foor loop: this is to add the correspondent alias to each column.
    SELECT 
        {% for col in dbt_utils.get_filtered_columns_in_relation(ref('stg_orders'), except=["memberid" ,"insert_date"]) %}
        o.{{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %},

        {% for col in dbt_utils.get_filtered_columns_in_relation(ref('stg_members'), except=["memberid", "source_name", "insert_date"]) %}
        m.{{ col }} {% if not loop.last %}, {% endif %}
        {% endfor %},

        {% for col in dbt_utils.get_filtered_columns_in_relation(ref('stg_marketing'), except=["campaignid", "source_name", "insert_date"]) %}
        mk.{{ col }} {% if not loop.last %}, {% endif %}
        {% endfor %},

        CURRENT_TIMESTAMP AS insert_date
    FROM 
        base_filtered o
    LEFT JOIN 
        {{ ref('stg_members') }} m ON o.memberid = m.memberid
    LEFT JOIN 
        {{ ref('stg_marketing') }} mk ON o.campaignid = mk.campaignid
)

SELECT 
    * 
FROM enriched_orders
