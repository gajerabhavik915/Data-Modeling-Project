"""
LCBO Settlement Reconciliation Pipeline — Airflow DAG

Option A: ONE DAG, three visual task groups (Bronze / Silver / Gold).
Ordering is guaranteed by the >> dependencies between the groups, so
Silver never starts before Bronze finishes, and Gold never starts
before Silver finishes.

Runs inside Docker. Paths below are the CONTAINER paths (where your
folders are mounted via docker-compose volumes), not your Windows paths.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator  # type: ignore[reportMissingImports]
from airflow.utils.task_group import TaskGroup  # type: ignore[reportMissingImports]
from datetime import datetime
from airflow.models import Variable
from dotenv.variables import Variable

# --- shared command prefixes -------------------------------------------------
# These are the paths INSIDE the Airflow container (from the volume mounts).
DBT = "cd /opt/airflow/dbt_project && dbt"
ETL = "python /opt/airflow/etl_scripts"

import os

DB_ENV = {
    "PG_HOST": "host.docker.internal",   # override: reach the Windows host's Postgres
    "PG_PORT": "5432",
    "PG_DBNAME": Variable.get("PG_DBNAME"),             # your data lives in the 'postgres' database
    "PG_USER": Variable.get("PG_USER"),               # match what your dbt docker target uses
    "PG_PASSWORD": Variable.get("PG_PASSWORD"),       # your real Postgres password
    "source_file_path": "/opt/airflow/source_files",
    "dim_products_file_path": "/opt/airflow/source_files/Master Product Cost",
    "chunk_size": "5000",
    "PATH": os.environ.get("PATH", ""),  # keep PATH so python resolves
}


default_args = {
    "owner": "bhavik",
    "retries": 0,          # keep 0 while learning, so failures surface immediately
}

with DAG(
    dag_id="medallion_settlement_pipeline",
    description="Bronze -> Silver -> Gold = settlement reconciliation pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # trigger manually for now (no automatic schedule)
    catchup=False,
    default_args=default_args,
    tags=["lcbo", "medallion", "dbt"],
) as dag:

    # ======================= BRONZE (Python ingestion) =======================
    with TaskGroup(group_id="Bronze_Ingestion") as bronze:
        audit = BashOperator(
            task_id="audit_batch_log",
            bash_command=f"{ETL}/bronze_batch_audit.py",
            env=DB_ENV,
            append_env=True  # keep the PATH so python resolves
        )
        raw = BashOperator(
            task_id="ingest_settlements",
            bash_command=f"{ETL}/bronze_lcbo_raw.py",
            env=DB_ENV,
            append_env=True  # keep the PATH so python resolves
        )
        product_cost = BashOperator(
            task_id="ingest_product_cost",
            bash_command=f"{ETL}/bronze_product_cost.py",
            env=DB_ENV,
            append_env=True  # keep the PATH so python resolves
        )
        # order within Bronze
        audit >> raw >> product_cost

    # ======================= SILVER (dbt run + snapshots) ====================
    with TaskGroup(group_id="Silver_Transform") as silver:
        silver_pre_snapshot = BashOperator(
            task_id="clean_cast_settlements",
            bash_command=(
                f"{DBT} run --target docker --select "
                "dbt_lcbo_raw dbt_product_cost "
                "dbt_lcbo_columns_trans dbt_lcbo_columns_type "
                "dbt_lcbo_cleansed_settlement dbt_lcbo_filterout_negative_sales "
                "dbt_dim_product_transform"
            ),
            env=DB_ENV,
            append_env=True  # keep the PATH so python resolves
        )
        snapshot_cost = BashOperator(
            task_id="snapshot_product_cost",
            bash_command=f"{DBT} snapshot --target docker --select dim_product_cost_snapshot",
            env=DB_ENV,
            append_env=True  # keep the PATH so python resolves
        )
        silver_joined = BashOperator(
            task_id="join_settlement_to_cost",
            bash_command=f"{DBT} run --target docker --select dbt_lcbo_joined_dim_prod_cost",
            env=DB_ENV,
            append_env=True  # keep the PATH so python resolves
        )
        snapshot_product = BashOperator(
            task_id="snapshot_product_dim",
            bash_command=f"{DBT} snapshot --target docker --select dim_product_snapshot",
            env=DB_ENV,
            append_env=True  # keep the PATH so python resolves
        )
        # order within Silver — snapshots sandwiched between the runs
        silver_pre_snapshot >> snapshot_cost >> silver_joined >> snapshot_product

    # ======================= GOLD (dbt run — star schema) ====================
    with TaskGroup(group_id="Gold_Star_Schema") as gold:
        gold_dims_and_fact = BashOperator(
            task_id="build_dims_and_fact",
            bash_command=(
                f"{DBT} run --target docker --select "
                "gold_dim_date gold_dim_product_cost gold_dim_product_info "
                "gold_dim_store dbt_lcbo_fact_table gold_fact_table" 
            ),
            env=DB_ENV,
            append_env=True  # keep the PATH so python resolves
        )

    # ======================= THE ORDER THAT MATTERS ==========================
    # This one line enforces Bronze -> Silver -> Gold.
    bronze >> silver >> gold
