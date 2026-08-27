{{
  config(
    target_schema='gold',
    materialized = 'table',
    tag = ['lcbo_project', 'dim_product_table_gold']
    )
}}

select dbt_scd_id as product_sk,
        *
from {{ ref('dim_product_cost_snapshot') }}
