{{
    config(
        materialized='table',
        tags=['silver_lcbo_transform_joined_dim_prod_cost']
    )
}}

select settlement.settlement_id,
    settlement.settlement_date,
    settlement.store_id,
    settlement.store_name,
    settlement.store_city,
    settlement.item_id,
    settlement.item_description,
    settlement.category,
    settlement.supplier_name,
    settlement.units_sold,
    settlement.unit_price,
    settlement.gross_sales,
    settlement.discount_amount,
    settlement.net_sales,
    settlement.cost_of_goods,
    settlement.gross_margin,



    prod_cost.cost_per_item as actual_cost_per_item,

    (settlement.units_sold * prod_cost.cost_per_item) as actual_cost_of_goods,

    (settlement.gross_sales - (settlement.units_sold * prod_cost.cost_per_item)) as dim_gross_margin,

    settlement.source_file,
    settlement.batch_id,
    settlement.ingested_at,
    settlement.is_active,
    settlement.file_hash


from {{ref('dbt_lcbo_cleansed_settlement')}} as settlement
left join {{ref('dim_product_cost_snapshot')}} as prod_cost                 -- joining with dim_prod_cost (snapshot) to get the cost per item for each settlement record   
on settlement.item_id = prod_cost.item_id
and settlement.settlement_date between prod_cost.dbt_valid_from and coalesce(prod_cost.dbt_valid_to, '9999-12-31')