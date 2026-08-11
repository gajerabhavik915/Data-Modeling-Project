{{
    config(
        materialized='table',
        schema='bronze'
    )
}}

# This model is used to extract the latest pending settlement records from the staging table `stg_lcbo_settlement'


with pipeline_run_log as (
    select max(run_id) as latest_run_id 
    from {{ source('lcbo_project', 'pipeline_run_log') }}
    where run_status = 'pending'
)

select 
    distinct(settlement.*)
from {{ source('lcbo_project', 'stg_lcbo_settlement') }} as settlement
    where settlement.run_id = (select latest_run_id from pipeline_run_log)