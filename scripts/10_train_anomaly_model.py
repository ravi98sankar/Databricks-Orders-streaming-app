"""
ORDER ANOMALY MODEL: Classical ML training, logging, and Unity Catalog registration.

Trains an unsupervised anomaly detector (IsolationForest) over silver_orders
joined to gold_customer_metrics, wraps it in a custom pyfunc so serving
returns a clean {anomaly_score, is_anomaly} shape instead of sklearn's raw
-1/1 predict()/unbounded decision_function(), and registers it to Unity
Catalog so it can be deployed via a Model Serving endpoint
(resources/ml_resources.yml).

Demo-scale honesty: the seed dataset is 7 orders / 4 customers. This proves
the train -> log -> register -> serve mechanism end-to-end; it is not a
production fraud model, and IsolationForest on this few rows won't find
meaningful anomalies -- that's expected, not a bug.

Run as the `train_anomaly_model` task in resources/ml_resources.yml.
"""

import argparse
import os
import sys

import mlflow
import mlflow.pyfunc
import pandas as pd
from pyspark.sql import SparkSession
from sklearn.ensemble import IsolationForest

from utils import setup_logger, log_step, log_metrics, get_row_count
from config import ANOMALY_MODEL_NAME

FEATURE_COLUMNS = [
    "amount",
    "deviation_from_avg",
    "order_hour",
    "order_dow",
    "is_vip_numeric",
    "total_orders_count",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Train the order anomaly detection model")
    parser.add_argument("--catalog", default=os.getenv("DBT_CATALOG", "main"))
    parser.add_argument("--schema", default=os.getenv("DBT_SCHEMA", "medallion_orders"))
    parser.add_argument("--experiment-id", default=os.getenv("MLFLOW_EXPERIMENT_ID", ""))
    return parser.parse_args()


def create_spark_session(app_name: str = "OrderAnomalyTraining") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


class AnomalyDetector(mlflow.pyfunc.PythonModel):
    """
    Wraps IsolationForest so serving returns a clean, documented output
    shape instead of sklearn's raw -1/1 predict() and unbounded
    decision_function().
    """

    def __init__(self, model: IsolationForest, feature_names: list):
        self.model = model
        self.feature_names = feature_names

    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        X = model_input[self.feature_names]
        scores = self.model.decision_function(X)
        raw_preds = self.model.predict(X)  # -1 = anomaly, 1 = normal
        return pd.DataFrame(
            {
                "anomaly_score": scores,
                "is_anomaly": raw_preds == -1,
            }
        )


def load_training_data(spark: SparkSession, catalog: str, schema: str, logger) -> pd.DataFrame:
    """Join silver_orders to gold_customer_metrics and pull to pandas (tiny dataset)."""
    silver_table = f"{catalog}.{schema}.silver_orders"
    gold_table = f"{catalog}.{schema}.gold_customer_metrics"
    logger.info(f"Reading {silver_table} joined to {gold_table}...")

    df_silver = spark.table(silver_table)
    df_gold = spark.table(gold_table).select(
        "user_id", "avg_order_amount", "total_orders_count", "is_vip"
    )
    df_joined = df_silver.join(df_gold, on="user_id", how="left")

    logger.info(f"✓ Training rows loaded: {get_row_count(df_joined)}")
    return df_joined.toPandas()


def engineer_features(pdf: pd.DataFrame, logger) -> pd.DataFrame:
    """Derive the numeric feature matrix used by the model."""
    pdf = pdf.copy()
    pdf["order_timestamp"] = pd.to_datetime(pdf["order_timestamp"])
    pdf["order_hour"] = pdf["order_timestamp"].dt.hour
    pdf["order_dow"] = pdf["order_timestamp"].dt.dayofweek
    pdf["deviation_from_avg"] = pdf["amount"] - pdf["avg_order_amount"]
    pdf["is_vip_numeric"] = pdf["is_vip"].astype(int)
    pdf[FEATURE_COLUMNS] = pdf[FEATURE_COLUMNS].fillna(0)

    logger.info(f"✓ Feature matrix ready: {len(pdf)} rows x {len(FEATURE_COLUMNS)} features")
    return pdf


def train_anomaly_model(spark: SparkSession, logger, catalog: str, schema: str, experiment_id: str) -> str:
    log_step(1, "ORDER ANOMALY MODEL - TRAIN, LOG, REGISTER", logger)

    pdf = load_training_data(spark, catalog, schema, logger)
    if len(pdf) < 4:
        logger.warning(
            f"⚠ Only {len(pdf)} training rows -- this is a demo-scale mechanism check, "
            "not a model with real predictive power."
        )

    pdf = engineer_features(pdf, logger)
    X = pdf[FEATURE_COLUMNS]

    mlflow.set_registry_uri("databricks-uc")
    if experiment_id:
        mlflow.set_experiment(experiment_id=experiment_id)

    registered_model_name = f"{catalog}.{schema}.{ANOMALY_MODEL_NAME}"

    with mlflow.start_run(run_name="order_anomaly_training"):
        contamination = min(0.2, max(1 / len(X), 0.05))
        model = IsolationForest(n_estimators=100, contamination=contamination, random_state=42)
        model.fit(X)

        wrapped = AnomalyDetector(model, FEATURE_COLUMNS)
        predictions = wrapped.predict(None, X)
        signature = mlflow.models.infer_signature(X, predictions)

        mlflow.log_params(
            {
                "n_estimators": 100,
                "contamination": contamination,
                "training_rows": len(X),
                "feature_columns": ",".join(FEATURE_COLUMNS),
            }
        )
        mlflow.log_metric("flagged_anomalies", int(predictions["is_anomaly"].sum()))

        mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=wrapped,
            signature=signature,
            input_example=X.head(min(3, len(X))),
            registered_model_name=registered_model_name,
        )

    logger.info(f"✓ Registered model: {registered_model_name}")

    log_metrics(
        {
            "layer": "order_anomaly_model",
            "training_rows": len(X),
            "flagged_anomalies": int(predictions["is_anomaly"].sum()),
            "registered_model_name": registered_model_name,
        },
        logger,
    )

    return registered_model_name


if __name__ == "__main__":
    args = parse_args()
    logger = setup_logger("OrderAnomalyTraining")
    spark = create_spark_session()

    try:
        model_name = train_anomaly_model(spark, logger, args.catalog, args.schema, args.experiment_id)
        logger.info(f"\n✅ Order anomaly model training complete! Registered as: {model_name}")
    except Exception as e:
        logger.error(f"❌ Order anomaly model training failed: {e}", exc_info=True)
        sys.exit(1)
