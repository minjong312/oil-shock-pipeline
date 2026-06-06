from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import sys

spark = SparkSession.builder \
    .appName("FX_Predict_RF_Model_Tuned") \
    .getOrCreate()

hdfs_proc_dir = "/user/maria_dev/fx_project/processed"

print("Step 1: Loading tuned ML dataset...")
try:
    data = spark.read.parquet(hdfs_proc_dir + "/ml_dataset")
except Exception as e:
    print("Error loading data.")
    sys.exit(1)

print("Step 2: Splitting data into Train and Test sets...")
train_data = data.filter(data["Date"] < "2024-01-01")
test_data = data.filter(data["Date"] >= "2024-01-01")

print("Step 3: Training Tuned Random Forest Classifier...")
# Tuned Parameters: numTrees=200, maxDepth=7
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=200, maxDepth=7, minInstancesPerNode=2, seed=42)
model = rf.fit(train_data)

print("Step 4: Evaluating Model Performance...")
predictions = model.transform(test_data)

evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator_acc.evaluate(predictions)

evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
f1_score = evaluator_f1.evaluate(predictions)

print("========================================")
print("Model Evaluation Results (Tuned Multi-class):")
print("Accuracy   : " + str(round(accuracy * 100, 2)) + "%")
print("Weighted F1: " + str(round(f1_score, 4)))
print("========================================")

print("Step 5: Extracting Feature Importances Ranking...")
importances = model.featureImportances.toArray()

feature_cols = []
for f in ["WTI", "DXY", "NewsVolume"]:
    for i in range(1, 6):
        feature_cols.append(f + "_lag" + str(i))
    feature_cols.append(f + "_MA5")
    feature_cols.append(f + "_STD5")

feat_imp_list = list(zip(feature_cols, importances))
feat_imp_list.sort(key=lambda x: x[1], reverse=True)

for i in range(len(feat_imp_list)):
    feat_name = feat_imp_list[i][0]
    feat_score = round(feat_imp_list[i][1], 4)
    print(str(i+1) + ". " + feat_name + " : " + str(feat_score))
print("========================================")

print("Done! Phase 5 Complete.")
spark.stop()

model_path = "hdfs:///user/maria_dev/fx_rf_model"
model.write().overwrite().save(model_path)
print(f"Model successfully saved to {model_path}")
