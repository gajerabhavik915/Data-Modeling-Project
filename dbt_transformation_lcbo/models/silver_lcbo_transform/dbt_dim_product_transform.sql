{{
    config(
        materialized='table',
        tag='dim_product_table_transform'
    )
}}

select distinct 
    {{ clean_item_id('product_cost.item_id') }} as item_id,
    product_cost.item_description,
    {{ clean_text('product_cost.category') }} as category,
    {{ clean_currency('product_cost.cost_per_item') }} as cost_per_item,
    {{ parse_multi_format_date('product_cost.cost_effective_date') }} as cost_effective_date

from {{ ref('dbt_product_cost') }} as product_cost