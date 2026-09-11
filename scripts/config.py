# Medallion Pipeline Configuration

import os

# Storage paths for Delta Lake layers (local/dry-run scripts only --
# the deployed dbt models write to Unity Catalog tables, not these paths)
BASE_PATH = "/tmp/databricks_lab_e2e"
BRONZE_PATH = f"{BASE_PATH}/bronze_orders"
SILVER_PATH = f"{BASE_PATH}/silver_orders"
GOLD_PATH = f"{BASE_PATH}/gold_customer_metrics"
CHECKPOINT_PATH = f"{BASE_PATH}/_checkpoints"

# Databricks configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "")

# Data quality thresholds
VIP_SPEND_THRESHOLD = 500.0
MIN_VALID_AMOUNT = 0.01
DUPLICATE_RETENTION = "latest"  # Keep latest record in dedup window

# LakeBase configuration
LAKEBASE_HOST = os.getenv("LAKEBASE_HOST", "")
LAKEBASE_PORT = int(os.getenv("LAKEBASE_PORT", "5432"))
LAKEBASE_DB = os.getenv("LAKEBASE_DB", "operational_db")
LAKEBASE_TABLE = "gold_user_metrics_lakebase"
LAKEBASE_USER = os.getenv("LAKEBASE_USER", "")
LAKEBASE_PASSWORD = os.getenv("LAKEBASE_PASSWORD", "")

# Logging and monitoring
LOG_LEVEL = "INFO"
ENABLE_METRICS = True
METRICS_NAMESPACE = "medallion_pipeline"

# ML / GenAI (scripts/10_train_anomaly_model.py, scripts/11_train_agent_explainer.py)
ANOMALY_MODEL_NAME = "order_anomaly_model"
EXPLAINER_MODEL_NAME = "order_anomaly_explainer"
# Confirmed current pay-per-token Foundation Model endpoint with broad Azure
# regional coverage -- still worth a one-time `databricks serving-endpoints
# list` check against the actual workspace before first run, since
# availability is workspace/region-dependent.
FM_ENDPOINT_NAME = os.getenv("FM_ENDPOINT_NAME", "databricks-meta-llama-3-3-70b-instruct")
