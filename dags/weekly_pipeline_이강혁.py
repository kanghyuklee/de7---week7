from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os


DATA_DIR = "/opt/airflow/data"
JOBS_DIR = "/opt/airflow/dags/jobs"


def download_from_s3(**kwargs):
    import boto3

    bucket = os.environ.get("S3_BUCKET", "de7-week7")
    s3 = boto3.client("s3")
    s3_key = "bronze/netflix_titles.csv"
    local_path = os.path.join(DATA_DIR, "netflix_titles.csv")
    os.makedirs(DATA_DIR, exist_ok=True)

    s3.download_file(bucket, s3_key, local_path)
    print(f"다운로드 완료: s3://{bucket}/{s3_key} → {local_path}")
    print(f"파일 크기: {os.path.getsize(local_path):,} bytes")


def upload_to_s3(**kwargs):
    import boto3

    bucket = os.environ.get("S3_BUCKET", "de7-week7")
    ds = kwargs["ds"]
    s3 = boto3.client("s3")
    local_dir = os.path.join(DATA_DIR, "silver")
    s3_prefix = f"silver/{ds}/"

    uploaded = 0
    for root, dirs, files in os.walk(local_dir):
        for fname in files:
            local_path = os.path.join(root, fname)
            rel = os.path.relpath(local_path, local_dir)
            s3_key = s3_prefix + rel.replace("\\", "/")
            s3.upload_file(local_path, bucket, s3_key)
            print(f"업로드: {s3_key}")
            uploaded += 1

    resp = s3.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)
    print(f"\n=== s3://{bucket}/{s3_prefix} 목록 ===")
    for obj in resp.get("Contents", []):
        print(f"  {obj['Key']}  ({obj['Size']:,} bytes)")
    print(f"총 {uploaded} 개 파일 업로드 완료")


with DAG(
    dag_id="weekly_pipeline_이강혁",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["q9"],
) as dag:

    t_download = PythonOperator(
        task_id="download_bronze",
        python_callable=download_from_s3,
    )

    t_transform = BashOperator(
        task_id="transform_spark",
        bash_command=(
            "spark-submit --master 'local[*]' "
            f"{JOBS_DIR}/transform.py "
            f"--input {DATA_DIR}/netflix_titles.csv "
            f"--output {DATA_DIR}/silver "
            "--year {{ params.year }}"
        ),
        params={"year": 2015},
    )

    t_upload = PythonOperator(
        task_id="upload_silver",
        python_callable=upload_to_s3,
    )

    t_download >> t_transform >> t_upload
