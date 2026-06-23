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
    .appName("Retail Analytics") \
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

df.createOrReplaceTempView("sales")

print("\nTOP CATEGORIES BY SALES\n")

category_sales = spark.sql("""
SELECT
    Category,
    ROUND(SUM(Sales),2) AS Total_Sales
FROM sales
GROUP BY Category
ORDER BY Total_Sales DESC
""")

category_sales.show()

category_sales.toPandas().to_csv(
    r"D:/Projects/Cloud Data Engineer Projects/Project_4_Retail_Data_Lake_Analytics/output/category_sales.csv",
    index=False
)

print("\nTOP REGIONS BY PROFIT\n")

region_profit = spark.sql("""
SELECT
    Region,
    ROUND(SUM(Profit),2) AS Total_Profit
FROM sales
GROUP BY Region
ORDER BY Total_Profit DESC
""")

region_profit.show()

region_profit.toPandas().to_csv(
    r"D:/Projects/Cloud Data Engineer Projects/Project_4_Retail_Data_Lake_Analytics/output/region_profit.csv",
    index=False
)

print("\nYEARLY REVENUE TREND\n")

yearly_revenue = spark.sql("""
SELECT
    Year,
    ROUND(SUM(Sales),2) AS Revenue
FROM sales
GROUP BY Year
ORDER BY Year
""")

yearly_revenue.show()

yearly_revenue.toPandas().to_csv(
    r"D:/Projects/Cloud Data Engineer Projects/Project_4_Retail_Data_Lake_Analytics/output/yearly_revenue.csv",
    index=False
)

print("\nCATEGORY WISE PROFIT\n")

category_profit = spark.sql("""
SELECT
    Category,
    ROUND(SUM(Profit),2) AS Total_Profit
FROM sales
GROUP BY Category
ORDER BY Total_Profit DESC
""")

category_profit.show()

category_profit.toPandas().to_csv(
    r"D:/Projects/Cloud Data Engineer Projects/Project_4_Retail_Data_Lake_Analytics/output/category_profit.csv",
    index=False
)

print("\nAnalytics CSV Files Generated Successfully!")
print("\nFiles Created:")
print("1. category_sales.csv")
print("2. region_profit.csv")
print("3. yearly_revenue.csv")
print("4. category_profit.csv")

spark.stop()