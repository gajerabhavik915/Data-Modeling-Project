{{
    config(
        materialized='table',
        tag='dim_product_table'
    )
}}

select 
    distinct * 
from {{ source('lcbo_project', 'dim_product_cost') }}
