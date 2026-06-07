
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel
import sys

spark = SparkSession.builder.appName("FX_Inference").getOrCreate()

try:
    data_path = "hdfs:///user/maria_dev/fx_project/processed/ml_dataset"
    df = spark.read.parquet(data_path)

    latest_data = df.orderBy(df["Date"].desc()).limit(1)

    model_path = "hdfs:///user/maria_dev/fx_rf_model"
    model = PipelineModel.load(model_path) 

    predictions = model.transform(latest_data)
    
    result = predictions.select("Date", "prediction").collect()[0]
    print("========================================")
    print(f"[Daily Inference Result] Date: {result['Date']}, Predicted Class: {result['prediction']}")
    print("========================================")

except Exception as e:
    print(f"Inference Error: {e}")
    sys.exit(1)

spark.stop()
