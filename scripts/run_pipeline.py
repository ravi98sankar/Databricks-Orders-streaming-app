"""
ORCHESTRATOR: End-to-End Pipeline Coordinator
Executes the complete medallion pipeline with error handling and reporting.
"""

import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession

from utils import setup_logger

# Import step functions
from scripts.scripts import (
    00_bronze_ingestion,
    01_silver_transformation,
    02_gold_aggregation,
    03_lakebase_sync,
)


def create_spark_session(app_name: str = "MedallionPipelineOrchestrator") -> SparkSession:
    """Initialize and return Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def main():
    """Execute complete medallion pipeline with orchestration and error handling."""
    
    logger = setup_logger("Orchestrator", level="INFO")
    spark = create_spark_session()
    
    logger.info("\n" + "="*80)
    logger.info("MEDALLION PIPELINE ORCHESTRATOR")
    logger.info("="*80)
    logger.info(f"Pipeline Start Time: {datetime.now()}")
    
    start_time = datetime.now()
    pipeline_status = "SUCCESS"
    failed_steps = []
    
    try:
        # Step 1: Bronze Ingestion
        try:
            logger.info("\n[1/4] Executing Bronze Layer Ingestion...")
            bronze_path = _01_bronze_ingestion.ingest_raw_orders(spark, logger)
            logger.info(f"✅ Bronze layer complete: {bronze_path}")
        except Exception as e:
            logger.error(f"❌ Bronze layer failed: {e}", exc_info=True)
            failed_steps.append("Bronze")
            pipeline_status = "FAILED"
            raise
        
        # Step 2: Silver Transformation
        try:
            logger.info("\n[2/4] Executing Silver Layer Transformation...")
            silver_path = _01_silver_transformation.transform_bronze_to_silver(spark, logger)
            logger.info(f"✅ Silver layer complete: {silver_path}")
        except Exception as e:
            logger.error(f"❌ Silver layer failed: {e}", exc_info=True)
            failed_steps.append("Silver")
            pipeline_status = "FAILED"
            raise
        
        # Step 3: Gold Aggregation
        try:
            logger.info("\n[3/4] Executing Gold Layer Aggregation...")
            gold_path = _02_gold_aggregation.aggregate_silver_to_gold(spark, logger)
            logger.info(f"✅ Gold layer complete: {gold_path}")
        except Exception as e:
            logger.error(f"❌ Gold layer failed: {e}", exc_info=True)
            failed_steps.append("Gold")
            pipeline_status = "FAILED"
            raise
        
        # Step 4: LakeBase Sync
        try:
            logger.info("\n[4/4] Executing LakeBase Operational Sync...")
            dry_run = os.getenv("DRY_RUN", "true").lower() == "true"
            sync_status = _03_lakebase_sync.sync_gold_to_lakebase(spark, logger, dry_run=dry_run)
            logger.info(f"✅ LakeBase sync complete: {sync_status}")
        except Exception as e:
            logger.error(f"❌ LakeBase sync failed: {e}", exc_info=True)
            failed_steps.append("LakeBase")
            pipeline_status = "FAILED"
            # Note: We don't raise here to allow partial pipeline success
    
    finally:
        # Summary and cleanup
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "="*80)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("="*80)
        logger.info(f"Status: {pipeline_status}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Start Time: {start_time}")
        logger.info(f"End Time: {end_time}")
        
        if failed_steps:
            logger.error(f"Failed Steps: {', '.join(failed_steps)}")
        else:
            logger.info("✅ All steps completed successfully!")
        
        # Stop Spark session
        spark.stop()
        
        # Exit with appropriate code
        return 0 if pipeline_status == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
