from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import sys

sys.path.append("/opt/airflow")

from extract.service import extract_all
from extract.utils.logging_config import configure_logging

configure_logging()


def extract_postgres_to_s3_task(**context):
    """Wrapper task to extract PostgreSQL tables to S3."""
    execution_date = context.get("ds")
    run_ts = context.get("ts_nodash")
    return extract_all(execution_date=execution_date, run_ts=run_ts, execution_type="s3")

DBT_PROJECT_DIR = "/opt/airflow/dags/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dags/dbt"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-eng-alerts@company.com"],
}

with DAG(
    dag_id="postgres_to_s3_and_dbt",
    description="Extract from PostgreSQL to S3, wait 5 minutes, then run dbt",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["postgres", "s3", "dbt", "snowflake"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    extract_postgres_to_s3 = PythonOperator(
        task_id="extract_postgres",
        python_callable=extract_postgres_to_s3_task,
    )

    wait_5_minutes = BashOperator(
        task_id="wait_5_minutes",
        bash_command="sleep 300",
    )

    check_dbt = BashOperator(
        task_id="check_dbt",
        bash_command="""
        echo "===== DBT VERSION ====="
        which dbt || true
        dbt --version || true

        echo "===== INSTALLED DBT PACKAGES ====="
        pip list | grep dbt || true

        echo "===== PROJECT DIRECTORY ====="
        ls -la /opt/airflow/dags/dbt || true
        """,
    )

    dbt_packages = BashOperator(
        task_id="dbt_packages",
        bash_command=f"""
        cd {DBT_PROJECT_DIR}
        dbt deps --profiles-dir .
        """,
    )

    run_staging = BashOperator(
        task_id="run_staging",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select stage --target snowflake --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_snapshot = BashOperator(
        task_id="run_snapshot",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt snapshot --target snowflake --profiles-dir {DBT_PROFILES_DIR} 2>&1
        """,
    )

    run_dimensions = BashOperator(
        task_id="run_dimensions",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select marts.dimension --target snowflake --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_facts = BashOperator(
        task_id="run_facts",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select marts.fact --target snowflake --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_marketing = BashOperator(
        task_id="run_marketing",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select marts.marketing --target snowflake --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_sales = BashOperator(
        task_id="run_sales",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select marts.sales --target snowflake --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt test --target snowflake --profiles-dir {DBT_PROFILES_DIR}
        """,
    )


    start >> extract_postgres_to_s3 >> wait_5_minutes >> check_dbt >> dbt_packages >> run_staging >> run_snapshot >> run_dimensions >> run_facts >> run_marketing >> run_sales >> dbt_test >> end
