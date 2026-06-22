from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_PROJECT_DIR = "/opt/airflow/dags/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dags/dbt"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    
    
}

with DAG(
    dag_id="dbt_trigger_dag",
    description="Run DBT models and tests",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["dbt", "snowflake"],
) as dag:


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
        bash_command="""
        cd /opt/airflow/dags/dbt
        dbt deps --profiles-dir .
        """
        )

    run_staging = BashOperator(
        task_id="run_staging",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select stage --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_intermediate = BashOperator(
        task_id="run_intermediate",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select int --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_snapshot = BashOperator(
        task_id="run_snapshot",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select snapshot --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_dimensions = BashOperator(
        task_id="run_dimensions",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select marts.dimension --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_facts = BashOperator(
        task_id="run_facts",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select marts.fact --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt test --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    (
           check_dbt
        >> dbt_packages   
        >> run_staging
        >> run_snapshot
        >> run_intermediate
        >> run_dimensions
        >> run_facts
        >> dbt_test
    )