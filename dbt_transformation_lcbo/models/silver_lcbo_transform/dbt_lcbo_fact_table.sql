{{
  config(
    materialized = 'table',
    tag = ['lcbo_project', 'dbt_lcbo_fact_table']

    )
}}

select 
  {{ dbt_utils.generate_surrogate_key(['transformed.settlement_id', 'transformed.settlement_date', 'transformed.store_id', 'transformed.item_id']) }} as settlement_row_id,
  transformed.settlement_id, 
  transformed.settlement_date, 
  dates.date_sk,
  transformed.item_id,
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
  transformed.actual_cost_of_goods, 
  transformed.dim_gross_margin, 
  transformed.source_file, 
  transformed.batch_id, 
  transformed.ingested_at, 
  transformed.is_active, 
  transformed.file_hash,
  case
    when store.store_sk is null or product.product_info_sk is null then 'True'
    else 'False'
  end as ghost_record

from {{ ref('dbt_lcbo_joined_dim_prod_cost') }} as transformed
left join {{ ref('gold_dim_store') }} as store
on transformed.store_id = store.store_id 
  
left join {{ ref('gold_dim_product_info') }} as product
on transformed.item_id = product.item_id and
  transformed.settlement_date >= product.dbt_valid_from::date and 
  transformed.settlement_date < coalesce(product.dbt_valid_to::date, '9999-12-31')

left join {{ ref('gold_dim_date') }} as dates
on transformed.settlement_date = dates.date_day
  