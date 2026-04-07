"""
Bot Detection Demo — Step 3: Model Training

Trains an XGBoost binary classifier on engineered features.
Logs experiment with MLflow and registers model in Unity Catalog.
Output: otto_demo.bot_detection.bot_detector (MLflow registered model)
"""
import subprocess, sys
subprocess.run([sys.executable, "-m", "pip", "install", "-q",
                "xgboost==3.0.0", "scikit-learn==1.6.1", "mlflow==2.22.1"],
               check=True)

import os

CATALOG = "otto_demo"
SCHEMA = "bot_detection"
FEATURES_TABLE = f"{CATALOG}.{SCHEMA}.bot_features"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.bot_detector"
EXPERIMENT_NAME = f"/Users/otto.li@databricks.com/bot-detection-demo"

if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
else:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.serverless(True).getOrCreate()

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    classification_report, roc_auc_score,
    precision_score, recall_score, f1_score
)
import xgboost as xgb

print(f"Spark version: {spark.version}")
print(f"Loading features from {FEATURES_TABLE}...")

# --- Load features ---
NUMERIC_FEATURES = [
    "num_requests", "session_duration_secs", "avg_time_between_requests_ms",
    "page_views", "click_count", "mouse_events", "form_submissions", "js_errors",
    "requests_per_minute", "clicks_per_page", "is_high_speed",
    "ua_risk_score", "geo_risk_score", "device_risk_score",
    "has_mouse_activity", "mouse_events_per_click", "js_execution_score",
    "click_through_depth", "missing_browser_signals", "tls_risk",
    "referrer_risk", "heuristic_bot_score",
]
LABEL = "is_bot"

df = spark.table(FEATURES_TABLE).select(NUMERIC_FEATURES + [LABEL]).toPandas()
print(f"Loaded {len(df):,} rows — bot rate: {df[LABEL].mean()*100:.1f}%")

X = df[NUMERIC_FEATURES].fillna(0)
y = df[LABEL]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"Train: {len(X_train):,}  Test: {len(X_test):,}")

# --- Configure MLflow ---
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT_NAME)

with mlflow.start_run(run_name="xgboost-bot-detector-v1") as run:
    params = {
        "n_estimators": 300,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "scale_pos_weight": (y_train == 0).sum() / (y_train == 1).sum(),
        "eval_metric": "auc",
        "use_label_encoder": False,
        "random_state": 42,
    }
    mlflow.log_params(params)

    model = xgb.XGBClassifier(**params)
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=50,
    )

    # --- Evaluate ---
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    metrics = {
        "roc_auc": roc_auc_score(y_test, y_prob),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "f1": f1_score(y_test, y_pred),
        "test_rows": len(X_test),
    }
    mlflow.log_metrics(metrics)

    print("\n=== Model Performance ===")
    for k, v in metrics.items():
        if isinstance(v, float):
            print(f"  {k}: {v:.4f}")
    print(classification_report(y_test, y_pred, target_names=["human", "bot"]))

    # Feature importance
    importance = pd.Series(
        model.feature_importances_, index=NUMERIC_FEATURES
    ).sort_values(ascending=False)
    print("\n=== Top 10 Features ===")
    print(importance.head(10).to_string())

    # Log feature importance as artifact
    importance.to_csv("/tmp/feature_importance.csv")
    mlflow.log_artifact("/tmp/feature_importance.csv")

    # --- Log model with input example ---
    input_example = X_test.head(3)
    signature = mlflow.models.infer_signature(X_train, y_prob)

    mlflow.xgboost.log_model(
        model,
        artifact_path="model",
        input_example=input_example,
        signature=signature,
        registered_model_name=MODEL_NAME,
    )

    print(f"\n✓ Model registered: {MODEL_NAME}")
    print(f"  Run ID: {run.info.run_id}")
    print(f"  ROC-AUC: {metrics['roc_auc']:.4f}")

# --- Tag the latest version ---
from mlflow.tracking import MlflowClient
client = MlflowClient()
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
client.set_model_version_tag(MODEL_NAME, latest.version, "stage", "champion")
client.set_model_version_tag(MODEL_NAME, latest.version, "use_case", "bot_detection")

print(f"  Model version: {latest.version} (tagged as champion)")
print(f"\nDone! Deploy serving endpoint with model: {MODEL_NAME}/versions/{latest.version}")
