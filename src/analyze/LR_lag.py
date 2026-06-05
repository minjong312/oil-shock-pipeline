
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("ExchangeRate_Regression_Lag").getOrCreate()

df_data = spark.read.csv("analyzed_result_csv/*.csv", header=False, inferSchema=True) \
    .toDF("Date", "Total_Events", "Material_Conflicts", "Avg_Goldstein", "ExchangeRate")

df_data = df_data.dropDuplicates(["Date"])

windowSpec = Window.orderBy("Date")

df_analyzed = df_data \
    .withColumn("Prev_ExchangeRate", lag("ExchangeRate", 1).over(windowSpec)) \
    .withColumn("Rate_Change", (col("ExchangeRate") - col("Prev_ExchangeRate")) / col("Prev_ExchangeRate") * 100) \
    .withColumn("Lag1_Total_Events", lag("Total_Events", 1).over(windowSpec)) \
    .withColumn("Lag1_Material_Conflicts", lag("Material_Conflicts", 1).over(windowSpec)) \
    .withColumn("Lag1_Avg_Goldstein", lag("Avg_Goldstein", 1).over(windowSpec))

df_clean = df_analyzed.dropna()

feature_list = ['Lag1_Total_Events', 'Lag1_Material_Conflicts', 'Lag1_Avg_Goldstein']

vecAssembler = VectorAssembler(inputCols=feature_list, outputCol="features")

lr = LinearRegression(featuresCol="features", labelCol="Rate_Change") \
    .setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

trainDF, testDF = df_clean.randomSplit([0.8, 0.2], seed=42)

pipeline = Pipeline(stages=[vecAssembler, lr])
pipelineModel = pipeline.fit(trainDF)

predDF = pipelineModel.transform(testDF)
predAndLabel = predDF.select("prediction", "Rate_Change")

print("\n==================================================")
print("  [Prediction vs Actual Sample with Time Lag]")
print("==================================================")
predAndLabel.show(5)

evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="Rate_Change", metricName="rmse")
rmse = evaluator.evaluate(predAndLabel)

print("==================================================")
print("[*] Model Prediction Error (RMSE): " + str(round(rmse, 4)))
print("==================================================\n")

spark.stop()
