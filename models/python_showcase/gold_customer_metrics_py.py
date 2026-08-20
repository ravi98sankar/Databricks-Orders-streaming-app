"""
Python-model showcase, gold layer.

Same outcome as models/gold/gold_customer_metrics.sql, authored as a dbt
Python model -- a direct port of scripts/02_gold_aggregation.py's window
aggregations and VIP logic.

Threshold mirrors dbt_project.yml's `vip_spend_threshold` var -- kept as a
local constant here because dbt Python models don't inherit the Jinja
`var()` context.
"""

VIP_SPEND_THRESHOLD = 500.0


def model(dbt, session):
    dbt.config(
        materialized="table",
        submission_method="serverless_cluster",
    )

    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    df_silver = dbt.ref("silver_orders_py")

    window_lifetime = (
        Window.partitionBy("user_id")
        .orderBy("order_timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    window_ytd = (
        Window.partitionBy("user_id", F.year("order_timestamp"))
        .orderBy("order_timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df_metrics = (
        df_silver.withColumn(
            "lifetime_cumulative_spend", F.sum("amount").over(window_lifetime)
        )
        .withColumn("ytd_cumulative_spend", F.sum("amount").over(window_ytd))
        .withColumn(
            "is_vip_customer",
            F.when(
                F.col("lifetime_cumulative_spend") >= VIP_SPEND_THRESHOLD, True
            ).otherwise(False),
        )
    )

    df_gold = df_metrics.groupBy("user_id").agg(
        F.max("lifetime_cumulative_spend").alias("total_lifetime_spend"),
        F.max("ytd_cumulative_spend").alias("current_ytd_spend"),
        F.count("order_id").alias("total_orders_count"),
        F.avg("amount").alias("avg_order_amount"),
        F.min("amount").alias("min_order_amount"),
        F.max("amount").alias("max_order_amount"),
        F.max("order_timestamp").alias("last_active_timestamp"),
        F.min("order_timestamp").alias("first_active_timestamp"),
        F.max("is_vip_customer").alias("is_vip"),
    ).withColumn("_aggregated_at", F.current_timestamp())

    return df_gold
