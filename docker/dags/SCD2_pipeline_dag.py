from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# dags/dbt_pipeline_dag.py

DBT_DIR = "/opt/airflow/banking_dbt"
DBT_CMD = f"cd {DBT_DIR} && dbt --no-write-json"

default_args = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dbt_pipeline_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Chạy thủ công hoặc đặt cron: e.g. "0 3 * * *" (3h sáng)
    catchup=False,
    tags=["dbt", "snowflake"],
) as dag:
    # 1. Build staging models (lấy dữ liệu thô từ RAW_STAGING)
    t1 = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD} run --profiles-dir .",
    )

    # 2. Chạy Snapshot (SCD Type 2 theo dõi đổi email, phone,...)
    t2 = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_CMD} snapshot --profiles-dir .",
    )

    # 3. Build marts / dimensions / facts
    t3 = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_CMD} run --select marts --profiles-dir .",
    )

    # Thứ tự thực thi
    t1 >> t2 >> t3