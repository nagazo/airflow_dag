import requests
from datetime import datetime
from pytz import timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import Row
import pandas as pd
import os

spark = SparkSession.builder \
    .appName("Load JSON Data") \
    .getOrCreate()

def load_data():
    url = 'http://172.31.6.21:8044/all'
    headers = {
        'accept': 'application/json'
    }
    r = requests.post(url, headers=headers, data={}) # 요청 메서드 확인
    if r.status_code == 200:
        d = r.json()  # JSON 데이터를 로드
        return d
    else:
        print(f"Error: {r.status_code} - {r.text}")
        return None

# 데이터 로드
data = load_data()
schema = StructType([
    StructField("num", IntegerType(), True),
    StructField("file_name", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("request_time", StringType(), True),
    StructField("label", StringType(), True),
    StructField("prediction_result", StringType(), True),
    StructField("prediction_time", StringType(), True)
])
ts = datetime.now(timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S')
if data:
    df = spark.createDataFrame(data, schema=schema)
    df = df.dropna(subset=["label", "prediction_result"]) # null 제거
    c = df.count() # 데이터 개수
    # 맞았는지 개수
    correct = df.filter(df["label"] == df["prediction_result"]).count()
    if c != 0:
        # 정확도
        acc = correct / c
        print(f"Accuracy: {acc}")

        result_row = Row(data_count=c, accuracy=acc, aggregation_time=ts)
        result_df = spark.createDataFrame([result_row])
        pandas_df = result_df.toPandas()

        path = '/home/ubuntu/tmp/dog_agg.csv'

        if os.path.exists(path): # 파일이 이미 있다면
            old_df = pd.read_csv(path)
            new_df = pd.concat([old_df, pandas_df])
            new_df.to_csv(path, index = False)
        else:
            pandas_df.to_csv(path, index = False)

        result_df.show(truncate=False)
        print(c, acc, ts)
    else:
        print("No data to calculate accuracy. Division by zero avoided.")
