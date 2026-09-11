"""
ORDER ANOMALY EXPLAINER AGENT: a GenAI agent, registered in Unity Catalog
and served the same way as the classical model in
scripts/10_train_anomaly_model.py.

Takes an order's feature values + the anomaly model's score/flag and asks a
Databricks Foundation Model for a plain-English explanation. Single-shot,
non-chat -- a plain mlflow.pyfunc.PythonModel is sufficient (no need for the
newer ResponsesAgent/ChatAgent interfaces, which target multi-turn/
tool-calling agents).

Run as the `train_explainer_agent` task in resources/ml_resources.yml,
after `train_anomaly_model`.
"""

import argparse
import os
import sys

import mlflow
import mlflow.pyfunc
import pandas as pd

from utils import setup_logger, log_step, log_metrics
from config import EXPLAINER_MODEL_NAME, FM_ENDPOINT_NAME, VIP_SPEND_THRESHOLD


def parse_args():
    parser = argparse.ArgumentParser(description="Train/register the order anomaly explainer agent")
    parser.add_argument("--catalog", default=os.getenv("DBT_CATALOG", "main"))
    parser.add_argument("--schema", default=os.getenv("DBT_SCHEMA", "medallion_orders"))
    parser.add_argument("--experiment-id", default=os.getenv("MLFLOW_EXPERIMENT_ID", ""))
    return parser.parse_args()


class ExplainerAgent(mlflow.pyfunc.PythonModel):
    """
    Single-shot explanation agent. `client` is deliberately NOT built in
    __init__ -- load_context() runs after the object is deserialized/loaded
    (locally, or inside the serving container), which is the right place for
    a live connection object that must not be pickled as part of the model.
    """

    def __init__(self, fm_endpoint: str, vip_threshold: float):
        self.fm_endpoint = fm_endpoint
        self.vip_threshold = vip_threshold
        self.client = None

    def load_context(self, context):
        from mlflow.deployments import get_deploy_client

        self.client = get_deploy_client("databricks")

    def _build_prompt(self, row) -> str:
        vip_label = "VIP" if row.get("is_vip_numeric") else "not VIP"
        return (
            "You are a fraud-analysis assistant explaining an automated anomaly "
            "detector's output to a non-technical reviewer. Be concise (2-3 "
            "sentences), factual, and reference the actual numbers given.\n\n"
            f"Order amount: ${row['amount']:.2f}\n"
            f"Customer's average order amount: ${row['avg_order_amount']:.2f}\n"
            f"Deviation from customer average: ${row['deviation_from_avg']:.2f}\n"
            f"Customer total orders: {row['total_orders_count']}\n"
            f"Customer status: {vip_label} (VIP threshold: ${self.vip_threshold:.2f} lifetime spend)\n"
            f"Anomaly detector score: {row['anomaly_score']:.4f} (lower = more anomalous)\n"
            f"Flagged as anomaly: {row['is_anomaly']}\n\n"
            "Explain in plain English why this order was (or was not) flagged."
        )

    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        if self.client is None:
            from mlflow.deployments import get_deploy_client

            self.client = get_deploy_client("databricks")

        explanations = []
        for _, row in model_input.iterrows():
            prompt = self._build_prompt(row)
            try:
                response = self.client.predict(
                    endpoint=self.fm_endpoint,
                    inputs={
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 200,
                    },
                )
                text = response["choices"][0]["message"]["content"]
            except Exception as e:
                # Degrade gracefully rather than fail the whole batch --
                # same "never crash the caller, surface the problem instead"
                # philosophy as scripts/03_lakebase_sync.py's dry-run fallback.
                text = f"[explanation unavailable: {e}]"
            explanations.append(text)

        return pd.DataFrame({"explanation": explanations})


def build_example_input() -> pd.DataFrame:
    """Representative row for signature inference -- not live data, doesn't
    need to be (predict() only reads whatever columns are present)."""
    return pd.DataFrame(
        [
            {
                "amount": 450.0,
                "avg_order_amount": 233.5,
                "deviation_from_avg": 216.5,
                "total_orders_count": 2,
                "is_vip_numeric": 1,
                "anomaly_score": -0.05,
                "is_anomaly": True,
            }
        ]
    )


def train_and_register_explainer(logger, catalog: str, schema: str, experiment_id: str) -> str:
    log_step(2, "ORDER ANOMALY EXPLAINER AGENT - TRAIN, LOG, REGISTER", logger)

    mlflow.set_registry_uri("databricks-uc")
    if experiment_id:
        mlflow.set_experiment(experiment_id=experiment_id)

    registered_model_name = f"{catalog}.{schema}.{EXPLAINER_MODEL_NAME}"
    example_input = build_example_input()

    with mlflow.start_run(run_name="order_anomaly_explainer_training"):
        agent = ExplainerAgent(fm_endpoint=FM_ENDPOINT_NAME, vip_threshold=VIP_SPEND_THRESHOLD)
        agent.load_context(None)

        logger.info(f"Smoke-testing Foundation Model endpoint: {FM_ENDPOINT_NAME}")
        try:
            example_output = agent.predict(None, example_input)
        except Exception as e:
            logger.error(
                f"❌ Foundation Model endpoint call failed during registration smoke test: {e}"
            )
            logger.error(
                f"   Check the endpoint exists in this workspace/region: "
                f"`databricks serving-endpoints list` should include '{FM_ENDPOINT_NAME}'."
            )
            raise

        signature = mlflow.models.infer_signature(example_input, example_output)
        mlflow.log_params({"fm_endpoint": FM_ENDPOINT_NAME})

        mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=agent,
            signature=signature,
            input_example=example_input,
            registered_model_name=registered_model_name,
        )

    logger.info(f"✓ Registered model: {registered_model_name}")
    logger.info(f"  Example explanation: {example_output['explanation'].iloc[0]}")

    log_metrics(
        {
            "layer": "order_anomaly_explainer",
            "fm_endpoint": FM_ENDPOINT_NAME,
            "registered_model_name": registered_model_name,
        },
        logger,
    )

    return registered_model_name


if __name__ == "__main__":
    args = parse_args()
    logger = setup_logger("OrderAnomalyExplainer")

    try:
        model_name = train_and_register_explainer(logger, args.catalog, args.schema, args.experiment_id)
        logger.info(f"\n✅ Explainer agent training complete! Registered as: {model_name}")
    except Exception as e:
        logger.error(f"❌ Explainer agent training failed: {e}", exc_info=True)
        sys.exit(1)
