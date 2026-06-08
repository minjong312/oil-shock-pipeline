#!/bin/bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export PYTHONIOENCODING=utf-8
PROJECT_DIR="/home/maria_dev/fx-predict-pipeline"

echo "Starting Daily Inference Pipeline..."
python3 ${PROJECT_DIR}/src/ingest/get_yfinance.py
python3 ${PROJECT_DIR}/src/ingest/get_gdelt.py
spark-submit ${PROJECT_DIR}/src/pipeline/preprocess.py
spark-submit ${PROJECT_DIR}/src/pipeline/inference.py

echo "Committing predictions to GitHub..."
cd ${PROJECT_DIR}
git add docs/predictions.txt
git commit -m "Auto-update: Daily FX prediction added"
git push origin main

echo "Inference Complete."
