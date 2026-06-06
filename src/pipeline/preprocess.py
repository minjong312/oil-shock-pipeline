from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, split, when, lag, avg, stddev
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
import sys

spark = SparkSession.builder \
    .appName("FX_Predict_Preprocess_Tuned") \
    .getOrCreate()

hdfs_raw_dir = "/user/maria_dev/fx_project/raw"
hdfs_proc_dir = "/user/maria_dev/fx_project/processed"

print("Step 1: Reading and filtering GDELT data...")
raw_gdelt = spark.read.text(hdfs_raw_dir + "/gdelt/*.export.CSV")
kor_gdelt = raw_gdelt.filter(col("value").contains("KOR"))

parsed_gdelt = kor_gdelt.withColumn("date_str", split(col("value"), "\t").getItem(1))
gdelt_vol = parsed_gdelt.groupBy("date_str").agg(count("*").alias("NewsVolume"))
gdelt_vol = gdelt_vol.withColumn("Date", to_date(col("date_str"), "yyyyMMdd")).drop("date_str")

print("Step 2: Reading YFinance data...")
yfin_df = spark.read.csv(hdfs_raw_dir + "/yfinance_data.csv", header=True, inferSchema=True)
yfin_df = yfin_df.withColumn("Date", to_date(col("Date")))

print("Step 3: Joining datasets...")
merged_df = yfin_df.join(gdelt_vol, on="Date", how="left").fillna({"NewsVolume": 0})

print("Step 4: Creating Advanced Time-Lag & Trend Features...")
w_order = Window.orderBy("Date")
# Window for past 5 days strictly (T-5 to T-1) to prevent data leakage
w_past5 = Window.orderBy("Date").rowsBetween(-5, -1)

merged_df = merged_df.withColumn("prev_fx", lag("USDKRW", 1).over(w_order))
merged_df = merged_df.withColumn("fx_change_pct", ((col("USDKRW") - col("prev_fx")) / col("prev_fx")) * 100)

# Multi-class Labeling: Spike(2.0), Plunge(0.0), Sideways(1.0)
merged_df = merged_df.withColumn("label", 
    when(col("fx_change_pct") > 0.5, 2.0)
    .when(col("fx_change_pct") < -0.5, 0.0)
    .otherwise(1.0)
)

features_base = ["WTI", "DXY", "NewsVolume"]
for f in features_base:
    # 1. Simple Lags
    for i in range(1, 6):
        merged_df = merged_df.withColumn(f + "_lag" + str(i), lag(f, i).over(w_order))
    # 2. Moving Average (Trend)
    merged_df = merged_df.withColumn(f + "_MA5", avg(f).over(w_past5))
    # 3. Volatility (Risk)
    merged_df = merged_df.withColumn(f + "_STD5", stddev(f).over(w_past5))

final_df = merged_df.dropna()

print("Step 5: Vector Assembling for ML...")
feature_cols = []
for f in features_base:
    for i in range(1, 6):
        feature_cols.append(f + "_lag" + str(i))
    feature_cols.append(f + "_MA5")
    feature_cols.append(f + "_STD5")

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
ml_ready_df = assembler.transform(final_df)
ml_ready_df = ml_ready_df.select("Date", "features", "label", "USDKRW")

print("Step 6: Saving tuned data to HDFS...")
ml_ready_df.write.mode("overwrite").parquet(hdfs_proc_dir + "/ml_dataset")

print("Done! Phase 4 Complete.")
spark.stop()
