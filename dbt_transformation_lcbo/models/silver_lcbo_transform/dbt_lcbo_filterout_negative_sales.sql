{{
    config(
        materialized='incremental',
        unique_key= ['item_id', 'settlement_id', 'settlement_date', 'store_id', 'units_sold', 'unit_price', 'gross_sales', 'net_sales', 'cost_of_goods', 'gross_margin'],
        incremental_strategy='merge',
        tags='silver_lcbo_filterout_negative_sales'
    )
}}

select settlement.*,
    'Unprocessed' as manual_intervention_required,
    current_timestamp as created_at
    from {{ ref('dbt_lcbo_columns_type') }} as settlement
where settlement.units_sold < 0
        or settlement.unit_price < 0 
        or settlement.gross_sales < 0 
        or settlement.net_sales < 0 
        or settlement.cost_of_goods < 0 
        or settlement.gross_margin < 0 