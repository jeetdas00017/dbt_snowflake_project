"""
enterprise_dwh_pipeline
=======================

End-to-end orchestration, scheduled every 4 hours:

  Postgres --(extract)--> S3 (Parquet)
                 |
                 v
        Redshift STG (COPY)
                 |
                 v
        dbt: staging models  (stg_*, int_*_current)
                 |
                 v
        dbt: snapshot         (SCD2 history: snap_customers, snap_products)
                 |
                 v
        dbt: dimension layer  (dim_customers, dim_products)
                 |
                 v
        dbt: fact layer       (fact_orders)
                 |
                 v
        dbt: validation tests (staging + dim + fact)
                 |
        +--------+---------+
        v                  v
  Sales Mart          Marketing Mart
        +--------+---------+
                 v
        dbt: validation tests (datamarts)
                 |
                 v
            PowerBI / Tableau

Deployment notes:
  - `extract/` and `load/` packages must be importable on the worker
    (e.g. copied into <DAGS_FOLDER>/ or installed as a package / via
    a plugin so they're on PYTHONPATH).
  - DBT_PROJECT_DIR / DBT_PROFILES_DIR point to the dbt project checked
    out on the worker (or baked into the Airflow image).
  - Credentials (PG_*, RS_*, AWS, REDSHIFT_COPY_IAM_ROLE) are injected
    as environment variables via Airflow Connections/Variables, NOT
    hardcoded.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from extract.postgres_to_s3 import extract_table_to_s3
from load.s3_to_redshift import load_table_to_stg

DBT_PROJECT_DIR = "/opt/airflow/dbt/enterprise_dwh"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-eng-alerts@company.com"],
}

with DAG(
    dag_id="enterprise_dwh_pipeline",
    description="Postgres -> S3 -> Redshift STG -> dbt (staging/SCD2/dim/fact) -> Sales & Marketing marts",
    default_args=default_args,
    schedule_interval="0 */4 * * *",   # every 4 hours
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dwh", "redshift", "dbt", "incremental", "scd2"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ===============================================================
    # STEP 1 — Extract: Postgres -> S3 (Parquet), one task per table
    # ===============================================================
    extract_customers_to_s3 = PythonOperator(
        task_id="extract_customers_to_s3",
        python_callable=extract_table_to_s3,
        op_kwargs={"table_name": "customers"},
    )

    extract_products_to_s3 = PythonOperator(
        task_id="extract_products_to_s3",
        python_callable=extract_table_to_s3,
        op_kwargs={"table_name": "products"},
    )

    extract_orders_to_s3 = PythonOperator(
        task_id="extract_orders_to_s3",
        python_callable=extract_table_to_s3,
        op_kwargs={"table_name": "orders"},
    )
'''
    # ===============================================================
    # STEP 2 — Load: S3 -> Redshift STG (TRUNCATE + COPY), per table
    # ===============================================================
    load_customers_to_stg = PythonOperator(
        task_id="load_customers_to_stg",
        python_callable=load_table_to_stg,
        op_kwargs={"table_name": "customers"},
    )

    load_products_to_stg = PythonOperator(
        task_id="load_products_to_stg",
        python_callable=load_table_to_stg,
        op_kwargs={"table_name": "products"},
    )

    load_orders_to_stg = PythonOperator(
        task_id="load_orders_to_stg",
        python_callable=load_table_to_stg,
        op_kwargs={"table_name": "orders"},
    )

    # ===============================================================
    # STEP 3 — dbt: staging models (stg_*, int_*_current)
    # ===============================================================
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select staging"
        ),
    )

    # ===============================================================
    # STEP 4 — dbt: snapshot (SCD2 history for customers & products)
    # ===============================================================
    dbt_snapshot_scd2 = BashOperator(
        task_id="dbt_snapshot_scd2",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt snapshot --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # ===============================================================
    # STEP 5 — dbt: dimension layer (dim_customers, dim_products)
    # ===============================================================
    dbt_run_dimensions = BashOperator(
        task_id="dbt_run_dimensions",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select marts.dimension"
        ),
    )

    # ===============================================================
    # STEP 6 — dbt: fact layer (fact_orders)
    # ===============================================================
    dbt_run_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select marts.fact"
        ),
    )

    # ===============================================================
    # STEP 7 — dbt: validation tests on staging / dimension / fact
    # ===============================================================
    dbt_test_core_layers = BashOperator(
        task_id="dbt_test_core_layers",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} "
            f"--select staging marts.dimension marts.fact"
        ),
    )

    # ===============================================================
    # STEP 8 — dbt: datamarts (Sales + Marketing), run in parallel
    # ===============================================================
    dbt_run_sales_mart = BashOperator(
        task_id="dbt_run_sales_mart",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select marts.sales"
        ),
    )

    dbt_run_marketing_mart = BashOperator(
        task_id="dbt_run_marketing_mart",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select marts.marketing"
        ),
    )

    # ===============================================================
    # STEP 9 — dbt: validation tests on datamarts
    # ===============================================================
    dbt_test_datamarts = BashOperator(
        task_id="dbt_test_datamarts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} "
            f"--select marts.sales marts.marketing"
        ),
    )
'''
    # ===============================================================
    # Dependencies
    # ===============================================================
    start >> extract_customers_to_s3 >> load_customers_to_stg
    start >> extract_products_to_s3 >> load_products_to_stg
    start >> extract_orders_to_s3 >> load_orders_to_stg

    [load_customers_to_stg, load_products_to_stg, load_orders_to_stg] >> dbt_run_staging

    dbt_run_staging >> dbt_snapshot_scd2 >> dbt_run_dimensions >> dbt_run_facts >> dbt_test_core_layers

    dbt_test_core_layers >> [dbt_run_sales_mart, dbt_run_marketing_mart] >> dbt_test_datamarts >> end
