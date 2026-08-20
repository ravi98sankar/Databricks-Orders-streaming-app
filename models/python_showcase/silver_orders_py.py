"""
Python-model showcase, silver layer.

Same outcome as models/silver/silver_orders.sql, authored as a dbt Python
model -- a direct port of scripts/01_silver_transformation.py's quality
filters, dedup window, and struct-flattening logic.

Thresholds mirror dbt_project.yml's `min_valid_amount` var -- kept as a
local constant here because dbt Python models don't inherit the Jinja
`var()` context.
"""

MIN_VALID_AMOUNT = 0.01


def model(dbt, session):
    dbt.config(
        materialized="table",
        submission_method="serverless_cluster",
    )

    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    df_bronze = dbt.ref("bronze_orders_py")

    df_filtered = df_bronze.filter(
        (F.col("order_id").isNotNull())
        & (F.col("user_id").isNotNull())
        & (F.col("amount").isNotNull())
        & (F.col("amount") > MIN_VALID_AMOUNT)
        & (F.col("status") != "CANCELLED")
    )

    window_dedup = Window.partitionBy("order_id").orderBy(F.col("order_timestamp").desc())

    df_dedup = (
        df_filtered.withColumn("row_num", F.row_number().over(window_dedup))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    df_silver = (
        df_dedup.withColumn("shipping_city", F.col("shipping_address.city"))
        .withColumn("shipping_state", F.col("shipping_address.state"))
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_processing_stage", F.lit("silver"))
        .drop("shipping_address", "_ingested_at", "_ingestion_source")
    )

    return df_silver
