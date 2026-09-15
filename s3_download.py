import argparse
import csv
import os

import boto3


def main():
    parser = argparse.ArgumentParser(description="S3 bronze/ 데이터 다운로드")
    parser.add_argument(
        "--bucket",
        default=os.environ.get("S3_BUCKET"),
        help="S3 버킷 이름 (또는 환경변수 S3_BUCKET)",
    )
    args = parser.parse_args()

    if not args.bucket:
        parser.error("버킷 이름이 필요합니다: --bucket 또는 S3_BUCKET 환경변수")

    s3 = boto3.client("s3")
    bucket = args.bucket
    prefix = "bronze/"

    # 1) bronze/ 객체 목록 + 크기
    print(f"=== s3://{bucket}/{prefix} 객체 목록 ===")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        size = obj["Size"]
        print(f"  {key}  ({size:,} bytes)")

    # 2) netflix_titles.csv 다운로드
    s3_key = f"{prefix}netflix_titles.csv"
    local_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, "netflix_titles.csv")

    print(f"\n=== 다운로드: s3://{bucket}/{s3_key} → {local_path} ===")
    s3.download_file(bucket, s3_key, local_path)
    print("다운로드 완료")

    # 3) 행 수 (헤더 제외, 따옴표 안 줄바꿈은 한 행)
    with open(local_path, encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # 헤더 건너뛰기
        row_count = sum(1 for _ in reader)

    print(f"\n=== 행 수 (헤더 제외): {row_count} ===")


if __name__ == "__main__":
    main()
