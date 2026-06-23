from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    year,
    month,
    quarter,
    round
)

spark = SparkSession.builder \
    .appName("Retail Data Lake") \
    .getOrCreate()

file_path = r"D:/Projects/Cloud Data Engineer Projects/Project_4_Retail_Data_Lake_Analytics/data/retail_sales_dataset.csv"

df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

df = df.dropna()

df = df.withColumn(
    "Order_Date",
    to_date(col("Order_Date"), "dd-MM-yyyy")
)

df = df.withColumn(
    "Profit_Margin",
    round((col("Profit") / col("Sales")) * 100, 2)
)

df = df.withColumn(
    "Year",
    year(col("Order_Date"))
)

df = df.withColumn(
    "Month",
    month(col("Order_Date"))
)

df = df.withColumn(
    "Quarter",
    quarter(col("Order_Date"))
)

output_path = r"D:/Projects/Cloud Data Engineer Projects/Project_4_Retail_Data_Lake_Analytics/output/retail_parquet"

spark.conf.set(
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
    "2"
)

df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(output_path)

print("Parquet Data Lake Created Successfully!")

spark.stop()