{{
    config(
        materialized='table',
        tags=['silver_lcbo_transform_cleansed_settlement']
}}

select settlement.*,
    current_timestamp() as created_at
    from {{ ref('dbt_lcbo_columns_trans') }} as settlement
where settlement.unit_sold >= 0
        or settlement.unit_price >= 0 
        or settlement.gross_sales >= 0 
        or settlement.net_sales >= 0 
        or settlement.cost_of_goods_sold >= 0 
        or settlement.gross_margin >= 0