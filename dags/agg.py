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

def get_conn():
  conn = pymysql.connect(host='172.31.6.21',
                            port=53306,
                            user = 'nagazo', password = '4444',
                            database = 'nagazodb',
                            cursorclass=pymysql.cursors.DictCursor)
  return conn

with DAG(
        'agg',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=1,
    description='worker',
    schedule_interval="*/30 * * * *",
    #schedule_interval="@once",
    start_date=datetime(2024, 10, 8),
    catchup=True,
    tags=['agg','db','update'],
) as dag:

    start=EmptyOperator(
        task_id="start"
    )
    end = EmptyOperator(
        task_id="end"
    )
    make_log = BashOperator(
            task_id='make.log',
            bash_command="""
            source /home/ubuntu/nagazoenv/bin/activate
            python /home/ubuntu/airflow/team_spark/test_naga.py
            """
    )
    go_db = BashOperator(
            task_id='go.db',
            bash_command="""
            source /home/ubuntu/nagazoenv/bin/activate
            python /home/ubuntu/airflow/team_spark/gotomaria.py
            """
    )
    start >> make_log >> go_db >> end
