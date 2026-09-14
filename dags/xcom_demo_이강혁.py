from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

def calculate_value(**kwargs):
    run_id = kwargs["run_id"]
    marker_key = f"xcom_marker_{run_id}"

    marker = Variable.get(marker_key, default_var=None)
    if marker is None:
        Variable.set(marker_key, "done")
        raise Exception("첫 번째 시도 의도적 실패 — 재시도 테스트")

    Variable.delete(marker_key)
    result = {"word_count": 42, "source": "xcom_demo"}
    logging.info("계산 결과: %s", result)
    return result


def use_value(**kwargs):
    ti = kwargs["ti"]
    pulled = ti.xcom_pull(task_ids="calculate")
    logging.info("xcom_pull 로 받은 값: %s", pulled)


with DAG(
    dag_id="xcom_demo_이강혁",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["q6"],
) as dag:

    t1 = PythonOperator(
        task_id="calculate",
        python_callable=calculate_value,
        retries=3,
        retry_delay=timedelta(seconds=5),
    )

    t2 = PythonOperator(
        task_id="use_result",
        python_callable=use_value,
    )

    t1 >> t2
