
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel
import pandas as pd
import plotly.express as px
import os
import sys

spark = SparkSession.builder \
    .appName("FX_Predict_Visualization_CM") \
    .getOrCreate()

try:
    print("Step 1: Loading model and dataset from HDFS...")
    model_path = "hdfs:///user/maria_dev/fx_rf_model"
    model = RandomForestClassificationModel.load(model_path)

    data_path = "/user/maria_dev/fx_project/processed/ml_dataset"
    data = spark.read.parquet(data_path)

    print("Step 2: Generating predictions for Test Data...")
    test_data = data.filter(data["Date"] >= "2024-01-01")
    predictions = model.transform(test_data)

    print("Step 3: Converting to Pandas DataFrame for Matrix Calculation...")
    select_cols = ["label", "prediction"]
    pdf = predictions.select(select_cols).toPandas()

    print("Step 4: Calculating Confusion Matrix...")
    # Define explicitly to maintain 3x3 grid even if a class is missing
    class_labels = [0.0, 1.0, 2.0]
    string_labels = ['Plunge (0.0)', 'Stable (1.0)', 'Spike (2.0)']
    
    cm = pd.crosstab(
        pdf['label'], 
        pdf['prediction'], 
        rownames=['Actual'], 
        colnames=['Predicted'], 
        dropna=False
    )
    
    # Reindex to ensure all classes are present in the matrix
    cm = cm.reindex(index=class_labels, columns=class_labels, fill_value=0)

    print("Step 5: Generating Interactive Plotly Heatmap...")
    fig = px.imshow(
        cm,
        labels=dict(x="Predicted Class", y="Actual Class", color="Count"),
        x=string_labels,
        y=string_labels,
        text_auto=True,
        aspect="auto",
        color_continuous_scale='Blues',
        title='Confusion Matrix: FX Direction Prediction'
    )

    fig.update_layout(
        title_font_size=20,
        xaxis_title_font_size=16,
        yaxis_title_font_size=16,
        plot_bgcolor='rgba(0,0,0,0)'
    )

    docs_dir = "/home/maria_dev/fx-predict-pipeline/docs"
    if not os.path.exists(docs_dir):
        os.makedirs(docs_dir)

    html_path = docs_dir + "/confusion_matrix.html"
    fig.write_html(html_path)
    print("Success: Visualization saved to {}".format(html_path))

except Exception as e:
    print("Visualization Error: {}".format(e))
    sys.exit(1)

spark.stop()
