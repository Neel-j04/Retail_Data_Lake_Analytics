import os

print("JAVA_HOME =", os.environ.get("JAVA_HOME"))

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Retail Data Lake") \
    .getOrCreate()

print("Spark Started Successfully!")

spark.stop()