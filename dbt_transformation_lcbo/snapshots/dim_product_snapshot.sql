{% snapshot dim_product_snapshot %}

{{
   config(
       target_schema='gold',
       unique_key='item_id',
       strategy='check',
       check_cols =['item_description', 'category', 'supplier_name']
   )
}}

select distinct item_id, item_description, category, supplier_name
from {{ ref('dbt_lcbo_joined_dim_prod_cost') }}

{% endsnapshot %}