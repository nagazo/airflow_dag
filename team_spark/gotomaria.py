import pymysql.cursors
import pandas as pd

def get_conn():
  conn = pymysql.connect(host='172.31.6.21',
                            port=53306,
                            user = 'nagazo', password = '4444',
                            database = 'nagazodb',
                            cursorclass=pymysql.cursors.DictCursor)
  return conn

def go_maria():
    df = pd.read_csv('/home/ubuntu/tmp/dog_agg.csv')
    df_last = df.tail(1)
    connection = get_conn()
    with connection:
        with connection.cursor() as cursor:
            sql = "INSERT INTO aggregation (data_count, accuracy, aggregation_time) VALUES (%s, %s, %s)"
            cursor.execute(sql, (int(df_last['data_count'].iloc[0]), float(df_last['accuracy'].iloc[0]), df_last['aggregation_time'].iloc[0]))
        connection.commit()
    return df_last

go_maria()
