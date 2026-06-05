from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("LR_5Day_Window_Forecast").getOrCreate()

df_data = spark.read.csv("analyzed_result_csv/*.csv", header=False, inferSchema=True) \
    .toDF("Date", "Total_Events", "Material_Conflicts", "Avg_Goldstein", "ExchangeRate")

df_data = df_data.orderBy("Date")

feature_cols = ["Total_Events", "Material_Conflicts", "Avg_Goldstein"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_vector = assembler.transform(df_data)

total_rows = df_vector.count()

train_df = df_vector.limit(total_rows - 5)
test_df = df_vector.orderBy(col("Date").desc()).limit(5).orderBy("Date")

lr = LinearRegression(featuresCol="features", labelCol="ExchangeRate")
lr_model = lr.fit(train_df)

predictions = lr_model.transform(test_df)

print("========================================")
print("Result of 5-Day Trend Based Exchange Rate Forecast")
print("========================================")
predictions.select("Date", "Total_Events", "Material_Conflicts", "ExchangeRate", "prediction").show()
print("========================================")

spark.stop()
