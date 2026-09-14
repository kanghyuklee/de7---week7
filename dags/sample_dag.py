from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime


def task_a():
    return "Hello from task_a"


def task_b():
    return "Hello from task_b"


with DAG(
    dag_id="sample_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["q3"],
) as dag:
    start = EmptyOperator(task_id="start")
    python_a = PythonOperator(task_id="python_a", python_callable=task_a)
    python_b = PythonOperator(task_id="python_b", python_callable=task_b)
    end = EmptyOperator(task_id="end")

    start >> python_a >> python_b >> end
