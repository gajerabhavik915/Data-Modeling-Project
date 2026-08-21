{{
    config(
        materialized='table',
        tags=['silver_lcbo_transform_joined_dim_prod_cost']
    )
}}

select settlement.*, 
    prod_cost.cost_per_item as dim_cost_per_item,

    (settlement.unit_sold * prod_cost.cost_per_item) as dim_cost_of_goods_sold_total,

    settlement.gross_sales - (settlement.unit_sold * prod_cost.cost_per_item) as dim_gross_margin,

from ref('dbt_lcbo_cleansed_settlement') as settlement
left join ref('dim_prod_cost') as prod_cost                 -- joining with dim_prod_cost (snapshot) to get the cost per item for each settlement record   
on settlement.item_id = prod_cost.item_id
and settlement.settlement_date between prod_cost.cost_effective_date and prod_cost.end_date