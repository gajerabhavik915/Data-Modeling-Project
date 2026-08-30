{{
  config(
    materialized = 'table',
    tag = ['lcbo_project', 'dbt_lcbo_fact_table']

    )
}}

select 
  transformed.settlement_id, 
  transformed.settlement_date, 
  store.store_sk as store_id,
  product.product_info_sk as product_id,
  transformed.units_sold, 
  transformed.unit_price, 
  transformed.gross_sales, 
  transformed.discount_amount, 
  transformed.net_sales, 
  transformed.cost_of_goods, 
  transformed.gross_margin, 
  transformed.actual_cost_per_item, 
  transformed.actual_cost_of_goods_sold_total, 
  transformed.dim_gross_margin, 
  transformed.source_file, 
  transformed.batch_id, 
  transformed.ingested_at, 
  transformed.is_active, 
  transformed.file_hash
from {{ ref('dbt_lcbo_joined_dim_prod_cost') }} as transformed
left join {{ ref('gold_dim_store') }} as store
on transformed.store_id = store.store_id and 
  
left join {{ ref('gold_dim_product_info') }} as product
on transformed.item_id = product.item_id and
  transformed.settlement_date >= product.dbt_valid_from and 
  transformed.settlement_date < coalesce(product.dbt_valid_to, '9999-12-31')
  