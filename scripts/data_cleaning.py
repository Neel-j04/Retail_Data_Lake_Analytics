from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("Retail Data Cleaning") \
    .getOrCreate()

file_path = r"D:\Projects\Cloud Data Engineer Projects\Project_4_Retail_Data_Lake_Analytics\data\retail_sales_dataset.csv"

df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

print("Before Cleaning:")
print("Rows:", df.count())

df = df.dropna()

df = df.withColumn(
    "Order_Date",
    to_date(col("Order_Date"), "dd-MM-yyyy")
)

df = df.dropDuplicates()

print("\nAfter Cleaning:")
print("Rows:", df.count())

print("\nUpdated Schema:")
df.printSchema()

df.show(5)

spark.stop()