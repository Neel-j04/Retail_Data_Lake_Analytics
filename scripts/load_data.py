from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Retail Data Lake Analytics") \
    .getOrCreate()

# Load Dataset
file_path = r"D:/Projects/Cloud Data Engineer Projects/Project_4_Retail_Data_Lake_Analytics/data/retail_sales_dataset.csv"

df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

print("\nDataset Loaded Successfully!")

print("\nTotal Rows:")
print(df.count())

print("\nTotal Columns:")
print(len(df.columns))

print("\nSchema:")
df.printSchema()

print("\nFirst 5 Records:")
df.show(5)

spark.stop()