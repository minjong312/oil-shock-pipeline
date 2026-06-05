from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, count, when, round

print("========================================")
print("Spark Job Started: HDFS Direct Processing")
print("========================================")

spark = SparkSession.builder.appName("GDELT_ExchangeRate_Direct").getOrCreate()

raw_gdelt = spark.read.csv("data/*.export.CSV", sep="\t")

gdelt_df = raw_gdelt.select(
    col("_c1").alias("SQLDATE"),
    col("_c29").cast("int").alias("QuadClass"),
    col("_c30").cast("float").alias("GoldsteinScale")
)

raw_exchange = spark.read.csv("raw_data/year=*/month=*/day=*/exchange_rate.csv", header=True)

exchange_df = raw_exchange.select(
    col("Date"),
    col("Close").cast("float").alias("ExchangeRate")
)

gdelt_processed = gdelt_df \
    .filter(col("SQLDATE").isNotNull()) \
    .withColumn("Date", to_date(col("SQLDATE"), "yyyyMMdd")) \
    .filter(col("Date").isNotNull()) \
    .groupBy("Date") \
    .agg(
        count("*").alias("Total_Events"),
        round(avg("GoldsteinScale"), 2).alias("Avg_Goldstein"),
        count(when(col("QuadClass") == 4, 1)).alias("Material_Conflicts")
    )

joined_df = exchange_df.join(gdelt_processed, "Date", "inner").orderBy("Date")

joined_df.write \
    .mode("overwrite") \
    .option("header", "false") \
    .csv("/user/maria_dev/analyzed_result_csv")

print("========================================")
print("Spark Job Completed Successfully!")
print("========================================")

spark.stop()
