"""
GOLD LAYER: Business Metrics, Aggregations, and VIP Identification
Produces customer-centric metrics and analytics from Silver layer.
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils import setup_logger, log_step, log_metrics, validate_dataframe, get_row_count
from config import SILVER_PATH, GOLD_PATH, VIP_SPEND_THRESHOLD


def create_spark_session(app_name: str = "GoldAggregation") -> SparkSession:
    """Initialize and return Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def calculate_cumulative_metrics(df, logger):
    """
    Calculate lifetime and YTD cumulative spend with window functions.
    """
    logger.info("Calculating cumulative spend metrics...")
    
    # Window specs for lifetime running spend
    window_lifetime = (
        Window.partitionBy("user_id")
        .orderBy("order_timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    
    # Window specs for YTD spend
    window_ytd = (
        Window.partitionBy("user_id", F.year("order_timestamp"))
        .orderBy("order_timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    
    df_metrics = (
        df.withColumn(
            "lifetime_cumulative_spend", F.sum("amount").over(window_lifetime)
        )
        .withColumn("ytd_cumulative_spend", F.sum("amount").over(window_ytd))
        .withColumn(
            "is_vip_customer",
            F.when(F.col("lifetime_cumulative_spend") >= VIP_SPEND_THRESHOLD, True).otherwise(False),
        )
    )
    
    logger.info("✓ Cumulative metrics calculated")
    return df_metrics


def aggregate_customer_summary(df, logger):
    """
    Create customer-level summary table with aggregated metrics.
    """
    logger.info("Aggregating customer summary metrics...")
    
    df_summary = df.groupBy("user_id").agg(
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
    
    logger.info("✓ Customer summary aggregation complete")
    return df_summary


def generate_gold_analytics(df, logger):
    """
    Generate additional analytical metrics.
    """
    logger.info("Generating analytical metrics...")
    
    vip_count = df.filter(F.col("is_vip") == True).count()
    total_customers = df.count()
    avg_lifetime_spend = df.agg(F.avg("total_lifetime_spend")).collect()[0][0]
    
    logger.info(f"  - Total unique customers: {total_customers}")
    logger.info(f"  - VIP customers: {vip_count} ({100*vip_count/total_customers:.1f}%)")
    logger.info(f"  - Average lifetime spend: ${avg_lifetime_spend:.2f}")
    
    return {
        "total_customers": total_customers,
        "vip_customers": vip_count,
        "avg_lifetime_spend": avg_lifetime_spend,
    }


def aggregate_silver_to_gold(spark: SparkSession, logger) -> str:
    """
    Main Gold layer aggregation pipeline.
    
    Returns:
        Path to Gold layer
    """
    log_step(3, "GOLD LAYER - BUSINESS METRICS & AGGREGATIONS", logger)
    
    # Read Silver layer
    logger.info(f"Reading Silver layer from: {SILVER_PATH}")
    df_silver = spark.read.format("delta").load(SILVER_PATH)
    silver_count = get_row_count(df_silver)
    logger.info(f"✓ Silver records loaded: {silver_count}")
    
    # Calculate cumulative metrics
    df_with_metrics = calculate_cumulative_metrics(df_silver, logger)
    
    # Create customer summary
    df_gold_summary = aggregate_customer_summary(df_with_metrics, logger)
    
    # Validate output
    if not validate_dataframe(df_gold_summary):
        logger.error("Gold layer validation failed")
        sys.exit(1)
    
    # Write to Gold Delta table
    df_gold_summary.write.format("delta").mode("overwrite").save(GOLD_PATH)
    gold_count = get_row_count(df_gold_summary)
    
    logger.info(f"\n✓ Gold layer persisted at: {GOLD_PATH}")
    logger.info(f"✓ Total customer records: {gold_count}")
    
    # Generate analytics
    analytics = generate_gold_analytics(df_gold_summary, logger)
    
    # Log metrics
    log_metrics({
        "layer": "gold",
        "input_records": silver_count,
        "output_records": gold_count,
        "unique_customers": gold_count,
        "vip_customers": analytics["vip_customers"],
        "avg_lifetime_spend": round(analytics["avg_lifetime_spend"], 2),
        "aggregation_timestamp": datetime.now().isoformat(),
    }, logger)
    
    # Display sample data
    logger.info("\nGold Layer Sample Data:")
    df_gold_summary.show(truncate=False)
    
    return GOLD_PATH


if __name__ == "__main__":
    logger = setup_logger("GoldAggregation")
    spark = create_spark_session()
    
    try:
        aggregate_silver_to_gold(spark, logger)
        logger.info("\n✅ Gold layer aggregation complete!")
    except Exception as e:
        logger.error(f"❌ Gold layer failed: {e}", exc_info=True)
        sys.exit(1)
