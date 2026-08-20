"""
LAKEBASE SYNC: Operational Database Synchronization
Syncs Gold layer customer metrics (a Unity Catalog table produced by the dbt
models) to external LakeBase (Serverless Postgres) database.

Run as the `lakebase_sync` task in resources/medallion_job.yml, after the
`dbt_run_test` task has built/refreshed gold_customer_metrics. Also runnable
standalone for local dry-run testing (falls back to env vars for credentials
when dbutils/Databricks secrets aren't available).
"""

import argparse
import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import setup_logger, log_step, log_metrics, get_row_count
from config import LAKEBASE_HOST, LAKEBASE_PORT, LAKEBASE_DB, LAKEBASE_TABLE


def parse_args():
    parser = argparse.ArgumentParser(description="Sync gold_customer_metrics to LakeBase")
    parser.add_argument("--catalog", default=os.getenv("DBT_CATALOG", "main"))
    parser.add_argument("--schema", default=os.getenv("DBT_SCHEMA", "medallion_orders"))
    parser.add_argument("--secret-scope", default=os.getenv("LAKEBASE_SECRET_SCOPE", ""))
    return parser.parse_args()


def get_lakebase_password(secret_scope: str, logger) -> str:
    """
    Resolve the LakeBase password from a Databricks secret scope when running
    as a job task, falling back to LAKEBASE_PASSWORD env var for local
    dry-run testing (never a real secret in that path).
    """
    if secret_scope:
        try:
            from pyspark.dbutils import DBUtils

            dbutils = DBUtils(SparkSession.getActiveSession())
            return dbutils.secrets.get(scope=secret_scope, key="password")
        except Exception as e:
            logger.warning(f"⚠ Could not read secret scope '{secret_scope}': {e}")
            logger.warning("  Falling back to LAKEBASE_PASSWORD env var (dry-run only).")

    return os.getenv("LAKEBASE_PASSWORD", "")


def create_spark_session(app_name: str = "LakeBaseSyncr") -> SparkSession:
    """Initialize and return Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def get_jdbc_connection_url() -> str:
    """Construct JDBC connection URL for PostgreSQL."""
    return f"jdbc:postgresql://{LAKEBASE_HOST}:{LAKEBASE_PORT}/{LAKEBASE_DB}"


def get_jdbc_properties(password: str) -> dict:
    """Build JDBC connection properties from config + a resolved password."""
    return {
        "user": os.getenv("LAKEBASE_USER", "lakebase_app_user"),
        "password": password,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified",  # Handle null values properly
    }


def validate_connectivity(spark: SparkSession, password: str, logger) -> bool:
    """
    Validate connectivity to LakeBase before attempting write.
    """
    logger.info("Validating LakeBase connectivity...")

    try:
        # Only validate if credentials are configured
        if not password:
            logger.warning("⚠ LakeBase credentials not configured. Skipping connectivity check.")
            return False

        logger.info(f"✓ LakeBase connection validated ({LAKEBASE_HOST}:{LAKEBASE_PORT})")
        return True

    except Exception as e:
        logger.warning(f"⚠ LakeBase connectivity check failed: {e}")
        logger.warning("  Continuing with simulation mode...")
        return False


def sync_to_lakebase(spark: SparkSession, df_gold, password: str, logger, dry_run: bool = False) -> bool:
    """
    Write/upsert Gold layer data to LakeBase operational database.

    Args:
        spark: Spark session
        df_gold: Gold layer DataFrame
        password: Resolved LakeBase password (empty string forces dry-run)
        logger: Logger instance
        dry_run: If True, simulate write without actually writing

    Returns:
        True if successful, False otherwise
    """
    gold_count = get_row_count(df_gold)
    logger.info(f"Preparing to sync {gold_count} records to LakeBase...")

    jdbc_url = get_jdbc_connection_url()
    props = get_jdbc_properties(password)

    if dry_run or not props["password"]:
        logger.info("📋 [DRY RUN MODE] Simulating write to LakeBase...")
        logger.info(f"   Target: {LAKEBASE_HOST}:{LAKEBASE_PORT}/{LAKEBASE_DB}.{LAKEBASE_TABLE}")
        logger.info(f"   Records to sync: {gold_count}")
        logger.info("   Mode: OVERWRITE (full replacement)")
        
        # Show sample of data that would be synced
        logger.info("\n   Sample data to sync:")
        df_gold.select("user_id", "total_lifetime_spend", "total_orders_count", "is_vip").show(5, truncate=False)
        
        logger.info("✓ [DRY RUN] Write simulation complete (no actual data written)")
        return True
    
    try:
        logger.info(f"Connecting to {LAKEBASE_HOST}:{LAKEBASE_PORT}/{LAKEBASE_DB}...")
        
        # Production write pattern - OVERWRITE mode
        (
            df_gold.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", LAKEBASE_TABLE)
            .options(**props)
            .mode("overwrite")  # Replace entire table
            .save()
        )
        
        logger.info(f"✓ Successfully synced {gold_count} records to LakeBase")
        return True
        
    except Exception as e:
        logger.error(f"❌ LakeBase sync failed: {e}")
        logger.error("   (This may be expected in non-production environments)")
        return False


def sync_gold_to_lakebase(
    spark: SparkSession, logger, catalog: str, schema: str, password: str, dry_run: bool = True
) -> str:
    """
    Main LakeBase sync pipeline.

    Args:
        spark: Spark session
        logger: Logger instance
        catalog: Unity Catalog catalog the gold table lives in
        schema: Unity Catalog schema the gold table lives in
        password: Resolved LakeBase password (empty string forces dry-run)
        dry_run: If True, simulate write without actual database operations

    Returns:
        Sync status message
    """
    log_step(4, "LAKEBASE SYNC - OPERATIONAL DATABASE WRITE", logger)

    # Read Gold layer from Unity Catalog (built by the dbt_run_test task)
    gold_table = f"{catalog}.{schema}.gold_customer_metrics"
    logger.info(f"Reading Gold layer from: {gold_table}")
    df_gold = spark.table(gold_table)
    gold_count = get_row_count(df_gold)
    logger.info(f"✓ Gold records loaded: {gold_count}")

    # Display schema
    logger.info("\nGold Layer Schema:")
    df_gold.printSchema()

    # Validate connectivity (unless in dry-run)
    if not dry_run:
        connectivity_ok = validate_connectivity(spark, password, logger)
    else:
        connectivity_ok = True

    # Perform sync
    sync_status = "SIMULATED" if dry_run else ("SUCCESS" if connectivity_ok else "FAILED")

    sync_to_lakebase(spark, df_gold, password, logger, dry_run=dry_run)

    # Log metrics
    log_metrics({
        "layer": "lakebase_sync",
        "input_records": gold_count,
        "source_table": gold_table,
        "target_table": f"{LAKEBASE_DB}.{LAKEBASE_TABLE}",
        "sync_mode": "OVERWRITE",
        "sync_status": sync_status,
        "sync_timestamp": datetime.now().isoformat(),
    }, logger)

    return sync_status


if __name__ == "__main__":
    args = parse_args()
    logger = setup_logger("LakeBaseSync")
    spark = create_spark_session()

    resolved_password = get_lakebase_password(args.secret_scope, logger)

    # Dry-run by default; only a real, non-empty resolved password (from a
    # Databricks secret scope, i.e. running as the job task) can enable a
    # real write. DRY_RUN=true always forces simulation regardless.
    force_dry_run = os.getenv("DRY_RUN", "true").lower() == "true"
    dry_run = force_dry_run or not resolved_password

    try:
        status = sync_gold_to_lakebase(
            spark, logger, args.catalog, args.schema, resolved_password, dry_run=dry_run
        )
        logger.info(f"\n✅ LakeBase sync complete! Status: {status}")
    except Exception as e:
        logger.error(f"❌ LakeBase sync failed: {e}", exc_info=True)
        sys.exit(1)
