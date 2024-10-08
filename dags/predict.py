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
        'predict',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=1,
    description='worker',
    schedule_interval="*/3 * * * *",
    #schedule_interval="@once",
    start_date=datetime(2024, 10, 8),
    catchup=True,
    tags=['predict','data'],
) as dag:

#    def load_data():
#        import requests
#        url="http://43.200.252.241:8044/all"
#        r=requests.get(url)
#        d=r.json()
#        return d

    def predict():
        from deep_fast.worker import run
        import pandas as pd
        import os
        li_result = run() # return 형식 = [label, time]
        print(li_result)
        # 데이터가 없는 경우
        if li_result == None:
            print('업데이트 할 데이터가 없습니다.')
            return False
        # 데이터가 있는 경우
        result = li_result[0] # 라벨
        pred_time = li_result[-1] # 예측 시간
        
#        data=load_data()
#        label=data['label']

        # 데이터 프레임 만들기
        df = pd.DataFrame({'prediction result': [result], 'pred_time': [pred_time]})
        # 로그파일을 csv 형태로 저장을 할꺼임
        # 데이터가 없다면 csv를 못읽을테니까 os.path.exist~ 하면 되겠죠
        file_path = "/home/ubuntu/tmp/result.csv"
        if os.path.exists(file_path):
            df1 = pd.read_csv(file_path)
            concat1 = pd.concat([df1, df])
            concat1.to_csv(file_path, index=False)
        else:
            df.to_csv(file_path, index=False)

        return result, pred_time
        

    start=EmptyOperator(
        task_id="start"
    )
    end = EmptyOperator(
        task_id="end"
    )
    worker = PythonVirtualenvOperator(
        task_id="worker",
        python_callable = predict,
        requirements=[
            "git+https://github.com/nagazo/deep_fast.git@manggee",
        ],
        system_site_packages=False,
        venv_cache_path = "/home/ubuntu/tmp/airflow_venv/get_data"
        # 시스템 패키지를 사용할지 여부
    )
    start >> worker >> end
