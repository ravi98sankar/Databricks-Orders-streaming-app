"""
SILVER LAYER: Data Quality, Deduplication, and Cleansing
Transforms Bronze raw data into cleansed, deduplicated Silver-layer records.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils import setup_logger, log_step, log_metrics, validate_dataframe, get_row_count
from config import BRONZE_PATH, SILVER_PATH, VIP_SPEND_THRESHOLD, MIN_VALID_AMOUNT, DUPLICATE_RETENTION


def create_spark_session(app_name: str = "SilverTransformation") -> SparkSession:
    """Initialize and return Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def apply_quality_filters(df, logger) -> int:
    """
    Apply data quality expectations to filter invalid records.
    
    Returns:
        Number of records filtered out
    """
    initial_count = get_row_count(df)
    
    df_filtered = df.filter(
        (F.col("order_id").isNotNull())
        & (F.col("user_id").isNotNull())
        & (F.col("amount").isNotNull())
        & (F.col("amount") > MIN_VALID_AMOUNT)
        & (F.col("status") != "CANCELLED")
    )
    
    filtered_count = get_row_count(df_filtered)
    records_removed = initial_count - filtered_count
    
    logger.info(f"Data Quality Filters Applied:")
    logger.info(f"  - Initial records: {initial_count}")
    logger.info(f"  - Filtered records: {filtered_count}")
    logger.info(f"  - Records removed: {records_removed}")
    
    return df_filtered, records_removed


def deduplicate_records(df, logger) -> int:
    """
    Deduplicate records keeping the latest per order_id.
    
    Returns:
        Number of duplicates removed
    """
    initial_count = get_row_count(df)
    
    window_dedup = Window.partitionBy("order_id").orderBy(
        F.col("order_timestamp").desc()
    )
    
    df_dedup = (
        df.withColumn("row_num", F.row_number().over(window_dedup))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    
    dedup_count = get_row_count(df_dedup)
    duplicates_removed = initial_count - dedup_count
    
    logger.info(f"Deduplication Applied:")
    logger.info(f"  - Records before dedup: {initial_count}")
    logger.info(f"  - Records after dedup: {dedup_count}")
    logger.info(f"  - Duplicates removed: {duplicates_removed}")
    
    return df_dedup, duplicates_removed


def flatten_and_enrich(df, logger):
    """Flatten nested structs and add audit columns."""
    df_flattened = (
        df.withColumn("shipping_city", F.col("shipping_address.city"))
        .withColumn("shipping_state", F.col("shipping_address.state"))
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_processing_stage", F.lit("silver"))
        .drop("shipping_address", "order_timestamp_str")
    )
    
    logger.info("✓ Struct flattening and enrichment complete")
    return df_flattened


def transform_bronze_to_silver(spark: SparkSession, logger) -> str:
    """
    Main Silver layer transformation pipeline.
    
    Returns:
        Path to Silver layer
    """
    log_step(2, "SILVER LAYER - DATA QUALITY & TRANSFORMATION", logger)
    
    # Read Bronze layer
    logger.info(f"Reading Bronze layer from: {BRONZE_PATH}")
    df_bronze = spark.read.format("delta").load(BRONZE_PATH)
    bronze_count = get_row_count(df_bronze)
    logger.info(f"✓ Bronze records loaded: {bronze_count}")
    
    # Apply data quality filters
    df_filtered, removed_quality = apply_quality_filters(df_bronze, logger)
    
    # Deduplicate records
    df_dedup, removed_dups = deduplicate_records(df_filtered, logger)
    
    # Flatten and enrich
    df_silver = flatten_and_enrich(df_dedup, logger)
    
    # Validate output
    if not validate_dataframe(df_silver):
        logger.error("Silver layer validation failed")
        sys.exit(1)
    
    # Write to Silver Delta table
    df_silver.write.format("delta").mode("overwrite").save(SILVER_PATH)
    silver_count = get_row_count(df_silver)
    
    logger.info(f"\n✓ Silver layer persisted at: {SILVER_PATH}")
    logger.info(f"✓ Total records written: {silver_count}")
    
    # Log metrics
    log_metrics({
        "layer": "silver",
        "input_records": bronze_count,
        "output_records": silver_count,
        "quality_filters_removed": removed_quality,
        "duplicates_removed": removed_dups,
        "data_quality_rate": round(100 * silver_count / bronze_count, 2),
        "processing_timestamp": datetime.now().isoformat(),
    }, logger)
    
    # Display sample data
    logger.info("\nSilver Layer Sample Data:")
    df_silver.show(truncate=False)
    
    return SILVER_PATH


if __name__ == "__main__":
    from datetime import datetime
    
    logger = setup_logger("SilverTransformation")
    spark = create_spark_session()
    
    try:
        transform_bronze_to_silver(spark, logger)
        logger.info("\n✅ Silver layer transformation complete!")
    except Exception as e:
        logger.error(f"❌ Silver layer failed: {e}", exc_info=True)
        sys.exit(1)
