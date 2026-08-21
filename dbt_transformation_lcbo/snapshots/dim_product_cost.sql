{% snapshot dim_product_cost_snapshot %}

{{
    
    config(
        target_schema='gold',
        unique_key='item_id',
        strategy='timestamp',
        updated_at='cost_effective_date',
        tags=['dim_product_cost_snapshot']
    )
}}

select * from {{ ref('dbt_dim_product_transform') }} as product_cost

{% endsnapshot %}