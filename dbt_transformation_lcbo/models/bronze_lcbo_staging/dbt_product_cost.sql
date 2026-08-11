{{
    config(
        materialized='table',
        schema='bronze'
    )
}}

select 
    distinct(*) 
from {{ source('lcbo_project', 'stg_product_cost') }}
