import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="입력 CSV 경로")
    parser.add_argument("--output", required=True, help="출력 parquet 경로")
    parser.add_argument("--year", type=int, default=2015, help="기준 연도")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("netflix_transform").getOrCreate()

    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("inferSchema", True)
        .csv(args.input)
    )

    df = df.filter(F.col("release_year") >= args.year)

    df = df.withColumn("genre", F.explode(F.split(F.col("listed_in"), ",")))
    df = df.withColumn("genre", F.trim(F.col("genre")))

    result = df.groupBy("type", "genre").count()

    row_count = result.count()

    result.write.mode("overwrite").option("compression", "snappy").parquet(args.output)

    print(f"집계 행 수: {row_count}")

    spark.stop()


if __name__ == "__main__":
    main()
