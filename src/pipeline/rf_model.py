import optuna
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import sys

spark = SparkSession.builder \
    .appName("FX_Predict_RF_Model_Optuna_Tuned") \
    .getOrCreate()

hdfs_proc_dir = "/user/maria_dev/fx_project/processed"

print("Step 1: Loading tuned ML dataset...")
try:
    data = spark.read.parquet(hdfs_proc_dir + "/ml_dataset")
except Exception as e:
    print(f"Error loading data. {e}")
    sys.exit(1)

print("Step 2: Splitting data into Train and Test sets...")
train_data = data.filter(data["Date"] < "2024-01-01")
test_data = data.filter(data["Date"] >= "2024-01-01")

train_data.cache()
test_data.cache()

print("Step 3: Hyperparameter Tuning with Optuna...")
def objective(trial):
    rf_numTrees = trial.suggest_int("numTrees", 50, 200)
    rf_maxDepth = trial.suggest_int("maxDepth", 5, 10)
    rf_minInstances = trial.suggest_int("minInstancesPerNode", 1, 5)

    rf = RandomForestClassifier(
        featuresCol="features", 
        labelCol="label", 
        numTrees=rf_numTrees, 
        maxDepth=rf_maxDepth, 
        minInstancesPerNode=rf_minInstances, 
        seed=42
    )
    temp_model = rf.fit(train_data)

    predictions = temp_model.transform(test_data)
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    f1_score = evaluator_f1.evaluate(predictions)

    return f1_score

study = optuna.create_study(direction="maximize")
print("Starting Optuna optimization trials...")
study.optimize(objective, n_trials=10)

print("========================================")
print("Optuna Best F1-Score :", round(study.best_value, 4))
print("Optuna Best Parameters :", study.best_params)
print("========================================")

print("Step 4: Training Final Model with Best Parameters...")
best_rf = RandomForestClassifier(
    featuresCol="features", 
    labelCol="label", 
    numTrees=study.best_params["numTrees"], 
    maxDepth=study.best_params["maxDepth"], 
    minInstancesPerNode=study.best_params["minInstancesPerNode"], 
    seed=42
)
final_model = best_rf.fit(train_data)

print("Step 5: Evaluating Final Model Performance...")
final_predictions = final_model.transform(test_data)

evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
final_accuracy = evaluator_acc.evaluate(final_predictions)

evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
final_f1_score = evaluator_f1.evaluate(final_predictions)

print("========================================")
print("Final Model Evaluation Results:")
print("Accuracy    : " + str(round(final_accuracy * 100, 2)) + "%")
print("Weighted F1 : " + str(round(final_f1_score, 4)))
print("========================================")

print("Step 6: Extracting Feature Importances Ranking...")
importances = final_model.featureImportances.toArray()

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

train_data.unpersist()
test_data.unpersist()


model_path = "hdfs:///user/maria_dev/fx_rf_model"
final_model.write().overwrite().save(model_path)
print(f"Final Model successfully saved to {model_path}")

spark.stop()
