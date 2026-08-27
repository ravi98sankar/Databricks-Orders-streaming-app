"""
Python-model showcase, bronze layer.

Same outcome as models/bronze/bronze_orders.sql, authored as a dbt Python
model (PySpark DataFrame API) instead of SQL -- a direct port of
scripts/00_bronze_ingestion.py into the dbt Python-model framework, so
engineers/leadership can compare both authoring styles for the same
pipeline. Lands in a distinct table (bronze_orders_py) so it doesn't
collide with the SQL model's output.

Landing path uses dbt.this (the relation this model itself materializes
into) for catalog/schema, not a hardcoded constant. Two earlier, wrong
attempts:
  1. A literal "medallion_orders" constant -- broke on the dev target,
     which actually runs against medallion_orders_dev.
  2. session.catalog.currentCatalog()/currentDatabase() -- this does NOT
     reflect the dbt_task's configured catalog/schema at all; it resolved
     to the workspace's generic "workspace.default", even though this same
     model correctly writes its own output to main.medallion_orders_dev
     (proof dbt itself knows the right target). dbt.this exposes exactly
     that same target dbt already resolved correctly.
"""

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

    landing_path = f"/Volumes/{dbt.this.database}/{dbt.this.schema}/landing/"

    df_raw = (
        session.read.format("csv")
        .option("header", "true")
        .schema(RAW_ORDERS_SCHEMA)
        .load(landing_path)
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
