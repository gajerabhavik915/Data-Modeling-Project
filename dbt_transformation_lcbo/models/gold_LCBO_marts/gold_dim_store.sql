{{
    config(
        materialized = 'table',
        tag = ['lcbo_project', 'dim_store_gold']
        )
}}


with CTE as (
select distinct store_id, store_name, store_city
from {{ ref('dbt_lcbo_joined_dim_prod_cost') }}
where store_id is not null and store_name is not null and store_city is not null
)

select {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_sk,
store_id, store_name, store_city
from CTE
