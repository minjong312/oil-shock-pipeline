from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel
import pandas as pd
import plotly.express as px
import os
import sys

spark = SparkSession.builder \
    .appName("FX_Predict_Visualization") \
    .getOrCreate()

try:
    print("Step 1: Loading trained Random Forest model from HDFS...")
    model_path = "hdfs:///user/maria_dev/fx_rf_model"
    model = RandomForestClassificationModel.load(model_path)

    print("Step 2: Extracting Feature Importances...")
    importances = model.featureImportances.toArray()

    feature_cols = []
    for f in ["WTI", "DXY", "NewsVolume"]:
        for i in range(1, 6):
            feature_cols.append(f + "_lag" + str(i))
        feature_cols.append(f + "_MA5")
        feature_cols.append(f + "_STD5")

    feat_imp_list = list(zip(feature_cols, importances))
    df_imp = pd.DataFrame(feat_imp_list, columns=['Feature', 'Importance'])

    print("Step 3: Generating Interactive Plotly Chart...")
    fig = px.bar(
        df_imp, 
        x='Importance', 
        y='Feature', 
        orientation='h',
        title='(Random Forest Feature Importance)',
        color='Importance', 
        color_continuous_scale='Blues'
    )

    fig.update_layout(
        yaxis={'categoryorder':'total ascending'},
        plot_bgcolor='rgba(0,0,0,0)',
        title_font_size=20
    )

    docs_dir = "/home/maria_dev/fx-predict-pipeline/docs"
    if not os.path.exists(docs_dir):
        os.makedirs(docs_dir)

    html_path = docs_dir + "/feature_importance.html"
    fig.write_html(html_path)
    print(f"Success: Visualization saved to {html_path}")

except Exception as e:
    print(f"Visualization Error: {e}")
    sys.exit(1)

spark.stop()
