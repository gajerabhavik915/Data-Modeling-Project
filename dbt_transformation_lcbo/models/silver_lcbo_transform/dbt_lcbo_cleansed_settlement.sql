{{
    config(
        materialized='table',
        tags=['silver_lcbo_transform_cleansed_settlement']
    )
}}

select settlement.*
from {{ ref('dbt_lcbo_columns_type') }} as settlement
where settlement.units_sold >= 0 
        and settlement.unit_price >= 0 
        and settlement.gross_sales >= 0 
        and settlement.net_sales >= 0 
        and settlement.cost_of_goods >= 0 
        and settlement.gross_margin >= 0