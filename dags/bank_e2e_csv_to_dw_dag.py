from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

DAG_ID = "bank_e2e_csv_to_dw"

INGEST_SCRIPT = "/opt/airflow/dags/scripts/load_csv_to_postgres_raw.py"
ETL_SCRIPT    = "/opt/airflow/dags/scripts/etl_bank_transform_load.py"
CHECK_SCRIPT  = "/opt/airflow/dags/scripts/post_run_check.py"

PYTHON = "python"

default_args = {"owner": "mindx_de", "retries": 2, "retry_delay": timedelta(minutes=1)}

DB_ENV = {
    "DB_HOST": os.getenv("DB_HOST"),
    "DB_PORT": os.getenv("DB_PORT"),
    "DB_NAME": os.getenv("DB_NAME"),
    "DB_USER": os.getenv("DB_USER"),
    "DB_PASS": os.getenv("DB_PASS"),
    "CSV_PATH": os.getenv("CSV_PATH"),
}

with DAG(
    dag_id=DAG_ID,
    description="E2E demo: CSV -> Postgres(raw) -> DW using ETL + Airflow",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["demo", "bank", "etl"],
) as dag:
    start = EmptyOperator(task_id="start")

    ingest = BashOperator(
        task_id="ingest_csv_to_raw",
        bash_command=f'{PYTHON} "{INGEST_SCRIPT}"',
        env=DB_ENV,
    )

    transform_load = BashOperator(
        task_id="transform_load_dw",
        bash_command=f'{PYTHON} "{ETL_SCRIPT}"',
        env=DB_ENV,
    )

    check = BashOperator(
        task_id="post_run_check",
        bash_command=f'{PYTHON} "{CHECK_SCRIPT}"',
        env=DB_ENV,
    )

    end = EmptyOperator(task_id="end")

    start >> ingest >> transform_load >> check >> end
