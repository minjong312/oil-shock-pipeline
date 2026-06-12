
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel
import os
import sys

spark = SparkSession.builder.appName("FX_Inference").getOrCreate()

try:
    data_path = "hdfs:///user/maria_dev/fx_project/processed/ml_dataset"
    df = spark.read.parquet(data_path)

    latest_data = df.orderBy(df["Date"].desc()).limit(1)

    model_path = "hdfs:///user/maria_dev/fx_rf_model"
    model = RandomForestClassificationModel.load(model_path)

    predictions = model.transform(latest_data)
    result = predictions.select("Date", "prediction").collect()[0]
    
    pred_val = result['prediction']
    if pred_val == 2.0:
        status_text = "급등 (Spike)"
    elif pred_val == 0.0:
        status_text = "급락 (Plunge)"
    else:
        status_text = "보합 (Sideways) - 환율 안정 예상"

    log_message = f"[{result['Date']}] 예측 결과: {status_text} (Class: {pred_val})"
    
    print("========================================")
    print(log_message)
    print("========================================")

    docs_dir = "/home/maria_dev/fx-predict-pipeline/docs"
    if not os.path.exists(docs_dir):
        os.makedirs(docs_dir)
        
    file_path = os.path.join(docs_dir, "predictions.txt")
    
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(log_message + "\n")

except Exception as e:
    print(f"Inference Error: {e}")
    sys.exit(1)

spark.stop()
