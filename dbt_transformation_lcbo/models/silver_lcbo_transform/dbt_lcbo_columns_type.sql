{{
    config(
        materialized='table',
        tag='silver_lcbo_columns_type'
    )
}}

-- in this code, we transform data type of numerical columns

select s.settlement_id,
    s.settlement_date,
    s.store_id::float as store_id,
    s.store_name,   
    s.store_city,
    s.item_id,
    s.item_description,
    s.category,
    s.supplier_name,
    s.units_sold::float as units_sold,
    s.unit_price,
    s.gross_sales,
    s.discount_amount,
    s.net_sales,
    s.cost_of_goods,
    s.gross_margin,

    s.source_file,
    s.batch_id,
    s.ingested_at,
    s.is_active,
    s.file_hash
from {{ ref('dbt_lcbo_columns_trans') }} as s