from datetime import datetime
from pytz import timezone
from deep_fast.db import get_conn, select, dml

import random
import requests
import os

def run():
    """image_processing 테이블을 읽어서 가장 오래된 요청 하나씩을 처리"""

    # STEP 1
    # image_processing 테이블의 prediction_result IS NULL 인 ROW 1 개 조회 - num >갖여오기
    connection = get_conn()
    with connection:
      with connection.cursor() as cursor:
            sql = "SELECT num, file_path, label FROM dog_class WHERE prediction_result IS NULL ORDER BY num LIMIT 1"
            cursor.execute(sql)
            result = cursor.fetchone() # 형식 : {'num' : ?}


    ts = datetime.now(timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S')
    if result == None:
        ind = None
        return ind
    else:
        ind = result['num']
        model=pipeline("image-classification", model="roschmid/dog-races")
        from PIL import Image
        from deep_fast.util_esther import get_max_label

        img = Image.open(result['file_path'])
        prediction = model(img)
        label = get_max_label(prediction)
        #score = prediction[0]['score']lab = result['label']
        connection = get_conn()
        with connection:
            with connection.cursor() as cursor:
                sql = f"UPDATE dog_class SET prediction_result={label}, prediction_time='{ts}' WHERE num={ind}"
                cursor.execute(sql)
            connection.commit()
        return prediction
