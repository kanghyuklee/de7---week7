from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, desc

spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

df = spark.read.text("/opt/spark-data/wordcount.txt")

words = df.select(explode(split(col("value"), "\\s+")).alias("word"))
words = words.filter(col("word") != "")

word_counts = words.groupBy("word").count().orderBy(desc("count"))

word_counts.show(20)

spark.stop()
