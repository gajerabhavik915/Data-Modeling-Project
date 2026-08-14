{{
    config(
        materialized ='table',
        tag ='settlement_table_raw'
    )
}}

-- This model is used to extract the latest pending settlement records from the staging table `stg_lcbo_settlement'


with latest_run as (
    select max(run_id) as latest_run_id 
    from {{ source('lcbo_project', 'pipeline_run_log') }}
    where status = 'pending'
)

select settlement.*
from {{ source('lcbo_project', 'stg_lcbo_settlement') }} as settlement
    where settlement.batch_id = (select latest_run_id from latest_run)