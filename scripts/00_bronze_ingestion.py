"""
BRONZE LAYER: Raw Data Ingestion
Generates or loads raw order data and writes to Delta Lake Bronze layer.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import setup_logger, log_step, log_metrics, validate_dataframe, get_row_count
from config import BRONZE_PATH


def create_spark_session(app_name: str = "BronzeIngestion") -> SparkSession:
    """Initialize and return Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def generate_raw_orders_data() -> list:
    """
    Generate mock raw orders dataset with intentional data quality issues.
    In production, this would be replaced with actual data sources (Kafka, S3, etc.).
    """
    return [
        (
            "ORD101",
            "U1001",
            150.50,
            "COMPLETED",
            "2026-03-01 10:00:00",
            {"city": "Austin", "state": "TX"},
        ),
        (
            "ORD102",
            "U1002",
            200.00,
            "PENDING",
            "2026-03-01 10:05:00",
            {"city": "Dallas", "state": "TX"},
        ),
        (
            "ORD101",
            "U1001",
            150.50,
            "COMPLETED",
            "2026-03-01 10:00:00",
            {"city": "Austin", "state": "TX"},
        ),  # Duplicate row
        (
            "ORD103",
            "U1001",
            450.00,
            "COMPLETED",
            "2026-03-01 11:30:00",
            {"city": "Austin", "state": "TX"},
        ),
        (
            "ORD104",
            "U1003",
            None,
            "CANCELLED",
            "2026-03-01 12:00:00",
            {"city": "Miami", "state": "FL"},
        ),  # Invalid row (null amount & cancelled)
        (
            "ORD105",
            "U1002",
            300.00,
            "COMPLETED",
            "2026-03-02 09:15:00",
            {"city": "Houston", "state": "TX"},
        ),
        (
            "ORD106",
            "U1004",
            1200.00,
            "COMPLETED",
            "2026-03-02 14:00:00",
            {"city": "Seattle", "state": "WA"},
        ),
    ]


def ingest_raw_orders(spark: SparkSession, logger) -> str:
    """
    Ingest raw orders data and write to Bronze Delta location.
    
    Returns:
        Path to Bronze layer
    """
    log_step(1, "BRONZE LAYER - RAW DATA INGESTION", logger)
    
    # Create DataFrame from mock data
    raw_orders_data = generate_raw_orders_data()
    
    df_raw = spark.createDataFrame(
        raw_orders_data,
        [
            "order_id",
            "user_id",
            "amount",
            "status",
            "order_timestamp_str",
            "shipping_address",
        ],
    )
    
    logger.info(f"Raw data ingested with {get_row_count(df_raw)} rows")
    
    # Add ingestion metadata
    df_bronze = df_raw.withColumn(
        "order_timestamp", F.col("order_timestamp_str").cast("timestamp")
    ).withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_ingestion_source", F.lit("mock_data_generator")
    )
    
    # Write to Bronze Delta table
    df_bronze.write.format("delta").mode("overwrite").save(BRONZE_PATH)
    
    # Validate output
    if not validate_dataframe(df_bronze):
        logger.error("Bronze layer validation failed")
        sys.exit(1)
    
    bronze_count = get_row_count(df_bronze)
    logger.info(f"✓ Bronze layer persisted at: {BRONZE_PATH}")
    logger.info(f"✓ Total records written: {bronze_count}")
    
    # Log metrics
    log_metrics({
        "layer": "bronze",
        "total_records": bronze_count,
        "ingestion_timestamp": datetime.now().isoformat(),
    }, logger)
    
    # Display sample data
    logger.info("\nBronze Layer Sample Data:")
    df_bronze.show(truncate=False)
    
    return BRONZE_PATH


if __name__ == "__main__":
    from datetime import datetime
    
    logger = setup_logger("BronzeIngestion")
    spark = create_spark_session()
    
    try:
        ingest_raw_orders(spark, logger)
        logger.info("\n✅ Bronze layer ingestion complete!")
    except Exception as e:
        logger.error(f"❌ Bronze layer failed: {e}", exc_info=True)
        sys.exit(1)
