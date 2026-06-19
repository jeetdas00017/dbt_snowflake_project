from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import sys

sys.path.append("/opt/airflow")

from extract.postgres_to_s3 import extract_table_to_s3

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-eng-alerts@company.com"],
}

with DAG(
dag_id="postgres_to_s3_test",
description="Test PostgreSQL -> S3 extraction",
start_date=datetime(2026, 1, 1),
schedule=None,
catchup=False,
tags=["test", "postgres", "s3"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    end = EmptyOperator(
        task_id="end"
    )

    extract_customers = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_table_to_s3,
        op_kwargs={
            "table_name": "stg_customers"
        }
    )

    extract_products = PythonOperator(
        task_id="extract_products",
        python_callable=extract_table_to_s3,
        op_kwargs={
            "table_name": "stg_products"
        }
    )

    extract_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_table_to_s3,
        op_kwargs={
            "table_name": "stg_orders"
        }
    )

    start >> [
        extract_customers,
        extract_products,
        extract_orders
    ] >> end