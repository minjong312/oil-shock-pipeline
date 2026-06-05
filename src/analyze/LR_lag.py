from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Daily_Data_Dashboard").getOrCreate()

raw_df = spark.read.csv("analyzed_result_csv/*.csv", header=False, inferSchema=True)

if raw_df.isEmpty():
    print("========================================")
    print("DataFrame is completely empty. Check ingestion or join logic.")
    print("========================================")
    spark.stop()
    exit(0)

df_data = raw_df.toDF("Date", "Total_Events", "Material_Conflicts", "Avg_Goldstein", "ExchangeRate")
df_data = df_data.dropDuplicates(["Date"]).orderBy("Date")

total_rows = df_data.count()

print("========================================")
print("Daily International Conflict & Exchange Rate Dashboard")
print("========================================")
if total_rows >= 5:
    df_data.orderBy(col("Date").desc()).limit(5).orderBy("Date").show(truncate=False)
else:
    df_data.show(truncate=False)
print("========================================")

spark.stop()
