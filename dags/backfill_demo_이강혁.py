from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import logging


def write_daily_report(**kwargs):
    ds = kwargs["ds"]
    output_dir = f"/opt/airflow/output/{ds}"
    os.makedirs(output_dir, exist_ok=True)

    filepath = os.path.join(output_dir, "report.txt")
    with open(filepath, "w") as f:
        f.write(f"Daily report for {ds}\n")
        f.write(f"Generated at: {kwargs['ts']}\n")
        f.write(f"DAG: backfill_demo_이강혁\n")

    logging.info("파일 생성 완료: %s", filepath)


with DAG(
    dag_id="backfill_demo_이강혁",
    start_date=datetime(2026, 9, 7),
    schedule="@daily",
    catchup=True,
    tags=["q7"],
) as dag:

    t1 = PythonOperator(
        task_id="write_report",
        python_callable=write_daily_report,
    )
