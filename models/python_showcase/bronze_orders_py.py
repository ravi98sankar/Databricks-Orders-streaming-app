"""
Python-model showcase, bronze layer.

Same outcome as models/bronze/bronze_orders.sql, authored as a dbt Python
model (PySpark DataFrame API) instead of SQL -- a direct port of
scripts/00_bronze_ingestion.py into the dbt Python-model framework, so
engineers/leadership can compare both authoring styles for the same
pipeline. Lands in a distinct table (bronze_orders_py) so it doesn't
collide with the SQL model's output.

Landing path/catalog/schema mirror the vars used by the SQL models
(dbt_project.yml `catalog`/`schema` vars) -- kept as local constants here
because dbt Python models don't inherit the Jinja `var()` context.
"""

CATALOG = "main"
SCHEMA = "medallion_orders"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing/orders/"

RAW_ORDERS_SCHEMA = (
    "order_id STRING, user_id STRING, amount STRING, status STRING, "
    "order_timestamp STRING, shipping_city STRING, shipping_state STRING"
)


def model(dbt, session):
    dbt.config(
        materialized="table",
        submission_method="serverless_cluster",
    )

    from pyspark.sql import functions as F

    df_raw = (
        session.read.format("csv")
        .option("header", "true")
        .schema(RAW_ORDERS_SCHEMA)
        .load(LANDING_PATH)
    )

    df_bronze = (
        df_raw.withColumn("amount", F.col("amount").cast("double"))
        .withColumn("order_timestamp", F.col("order_timestamp").cast("timestamp"))
        .withColumn(
            "shipping_address",
            F.struct(
                F.col("shipping_city").alias("city"),
                F.col("shipping_state").alias("state"),
            ),
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_source", F.lit("python_showcase_autoloader"))
        .drop("shipping_city", "shipping_state")
    )

    return df_bronze
