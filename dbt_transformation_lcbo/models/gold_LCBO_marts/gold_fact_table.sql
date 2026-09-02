{{
  config(
    materialized='incremental',
    target_schema = 'gold',
    unique_key='settlement_row_id',
    incremental_strategy='merge',
    on_schema_change = 'sync_all_columns',
    post_hook="
        update bronze.pipeline_run_log
        set
            completed_at = current_timestamp,
            status = 'success',
            rows_loaded = (select count(*) from {{ this }} where batch_id = (select max(run_id) from bronze.pipeline_run_log))
            
        where run_id = (
            select max(run_id)
            from bronze.pipeline_run_log
        );
    "
    )
}}

select 
  settlement_row_id,
  settlement_id, 
  settlement_date, 
  date_sk, 
  store_id,
  product_id,
  units_sold, 
  unit_price, 
  gross_sales, 
  discount_amount, 
  net_sales, 
  cost_of_goods, 
  gross_margin, 
  actual_cost_per_item, 
  actual_cost_of_goods, 
  dim_gross_margin, 
  source_file, 
  batch_id, 
  ingested_at, 
  is_active, 
  file_hash
from {{ ref('dbt_lcbo_fact_table') }}
where ghost_record = 'False'