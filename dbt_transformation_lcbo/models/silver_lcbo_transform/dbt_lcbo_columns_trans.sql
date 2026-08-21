{{
    config(
        materialized='table',
        tag= 'settlement_transform'
    )
}}

-- applied distinct to remove duplicates row from the raw settlement table.
-- applied macro's function to remove '.0' from the settlement_id, store_id and item_id columns.
-- applied macro's function to clean the unit_price and gross_sales columns to remove any currency symbols and convert them to numeric values.
-- applied macro's function to clean store_name, store_city, item_description, category and supplier_name columns to remove any leading or trailing whitespace.

SELECT Distinct

    {{ clean_item_id('settlement.settlement_id') }} as settlement_id,

    {{ parse_multi_format_date('settlement.settlement_date') }} as settlement_date,

    {{ clean_item_id('settlement.store_id') }} as store_id,
    {{ clean_text('settlement.store_name') }} as store_name,
    {{ clean_text('settlement.store_city') }} as store_city,

    {{ clean_item_id('settlement.item_id') }} as item_id,
    
    {{ clean_text('settlement.item_description') }} as item_description,
    {{ clean_text('settlement.category') }} as category,
    {{ clean_text('settlement.supplier_name') }} as supplier_name,

    settlement.units_sold, 

    {{ clean_currency('settlement.unit_price') }} as unit_price,
    {{ clean_currency('settlement.gross_sales') }} as gross_sales,

    settlement.discount_amount,

    {{ clean_currency('settlement.net_sales') }} as net_sales,
    {{ clean_currency('settlement.cost_of_goods') }} as cost_of_goods,
    {{ clean_currency('settlement.gross_margin') }} as gross_margin,

    settlement.source_file,
    settlement.batch_id,
    settlement.ingested_at,
    settlement.is_active,
    settlement.file_hash

FROM {{ ref('dbt_lcbo_raw') }} as settlement    