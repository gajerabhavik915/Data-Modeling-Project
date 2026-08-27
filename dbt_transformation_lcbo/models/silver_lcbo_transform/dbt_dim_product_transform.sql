{{
    config(
        materialized='table',
        tag='dim_product_table_transform'
    )
}}

select distinct 
    {{('product_cost.item_id') }} as item_id,
    product_cost.item_description,
    {{('product_cost.category') }} as category,
    product_cost.cost_per_item::numeric as cost_per_item,
    {{('product_cost.cost_effective_date') }} as cost_effective_date

from {{ ref('dbt_product_cost') }} as product_cost