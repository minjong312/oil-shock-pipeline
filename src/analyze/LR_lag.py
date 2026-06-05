from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import sys

spark = SparkSession.builder.appName("ExchangeRate_Regression").getOrCreate()

df_data = spark.read.csv("analyzed_result_csv/*.csv", header=False, inferSchema=True) \
    .toDF("Date", "Total_Events", "Material_Conflicts", "Avg_Goldstein", "ExchangeRate")


windowSpec = Window.orderBy("Date")
df_analyzed = df_data.withColumn("Prev_ExchangeRate", lag("ExchangeRate", 1).over(windowSpec)) \
    .withColumn("Rate_Change", (col("ExchangeRate") - col("Prev_ExchangeRate")) / col("Prev_ExchangeRate") * 100)

df_clean = df_analyzed.dropna()

windowSpecLag = Window.orderBy("Date")
df_lagged = df_clean.withColumn("Goldstein_lag1", lag("Avg_Goldstein", 1).over(windowSpecLag)) \
                    .withColumn("Goldstein_lag2", lag("Avg_Goldstein", 2).over(windowSpecLag)) \
                    .withColumn("Conflict_lag1", lag("Material_Conflicts", 1).over(windowSpecLag)) \
                    .withColumn("Conflict_lag2", lag("Material_Conflicts", 2).over(windowSpecLag))

df_lagged_clean = df_lagged.dropna()

feature_list = ['Goldstein_lag1', 'Goldstein_lag2', 'Conflict_lag1', 'Conflict_lag2']

vecAssembler = VectorAssembler(inputCols=feature_list, outputCol="features")

lr = LinearRegression(featuresCol="features", labelCol="Rate_Change") \
    .setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

trainDF, testDF = df_lagged_clean.randomSplit([0.8, 0.2], seed=42)

pipeline = Pipeline(stages=[vecAssembler, lr])
pipelineModel = pipeline.fit(trainDF)

predDF = pipelineModel.transform(testDF)
predAndLabel = predDF.select("prediction", "Rate_Change")

print("\n" + "="*50)
print("  [Prediction vs Actual Sample]")
print("="*50)
predAndLabel.show(5)

evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="Rate_Change", metricName="rmse")
rmse = evaluator.evaluate(predAndLabel)

print("="*50)
print("[*] Model Prediction Error (RMSE): {:.4f}".format(rmse))
print("="*50 + "\n")
