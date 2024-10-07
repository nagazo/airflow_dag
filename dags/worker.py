from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    BranchPythonOperator,
)

with DAG(
        'worker',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='worker',
    schedule_interval="*/3 * * * *",
    start_date=datetime(2023, 9, 26),
    catchup=True,
    tags=['worker','data'],
) as dag:


    start=EmptyOperator(
        task_id="start"
    )
    end = EmptyOperator(
        task_id="end"
    )

    worker = PythonVirtualenvOperator(
        task_id="worker",
        python_callable = worker,
        requirements=[
            "",
        ],
        system_site_packages=False,  # 시스템 패키지를 사용할지 여부
        python_version='3.11',
    )
    start >> worker >> end
