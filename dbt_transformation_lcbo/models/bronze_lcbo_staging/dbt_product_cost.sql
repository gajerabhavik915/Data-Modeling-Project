{{
    config(
        materialized='table',
        tag='dim_product_table'
    )
}}

select 
    distinct item_id::text as item_id, 
    item_description,
    category,
    cost_per_item,
    cost_effective_date
from {{ source('lcbo_project', 'dim_product_cost') }}
