"""
Unit tests for Silver Layer transformation.
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from scripts.scripts import _01_silver_transformation
from config import SILVER_PATH


class TestSilverTransformation(unittest.TestCase):
    """Test cases for Silver layer."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize Spark session for tests."""
        cls.spark = SparkSession.builder.appName("SilverTests").getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Stop Spark session."""
        cls.spark.stop()
    
    def test_quality_filters_remove_invalid_records(self):
        """Test that quality filters remove records with invalid data."""
        from utils import setup_logger
        
        logger = setup_logger("SilverTest")
        
        # Create test data with known invalid records
        test_data = [
            ("ORD001", "U001", 100.0, "COMPLETED", "2026-01-01 10:00:00", {"city": "Austin", "state": "TX"}),
            ("ORD002", "U002", None, "COMPLETED", "2026-01-01 11:00:00", {"city": "Dallas", "state": "TX"}),  # null amount
            ("ORD003", "U003", 0.0, "COMPLETED", "2026-01-01 12:00:00", {"city": "Houston", "state": "TX"}),  # zero amount
            ("ORD004", None, 150.0, "COMPLETED", "2026-01-01 13:00:00", {"city": "Austin", "state": "TX"}),  # null user_id
            ("ORD005", "U004", 200.0, "CANCELLED", "2026-01-01 14:00:00", {"city": "Miami", "state": "FL"}),  # cancelled
        ]
        
        df_test = self.spark.createDataFrame(
            test_data,
            ["order_id", "user_id", "amount", "status", "order_timestamp_str", "shipping_address"]
        ).withColumn(
            "order_timestamp", F.col("order_timestamp_str").cast("timestamp")
        )
        
        # Apply filters
        df_filtered, removed = _01_silver_transformation.apply_quality_filters(df_test, logger)
        
        self.assertEqual(removed, 4, "Should remove 4 invalid records")
        self.assertEqual(df_filtered.count(), 1, "Should have 1 valid record")
    
    def test_deduplication_keeps_latest(self):
        """Test that deduplication keeps latest record per order_id."""
        from utils import setup_logger
        
        logger = setup_logger("SilverTest")
        
        # Create test data with duplicates
        test_data = [
            ("ORD001", "U001", 100.0, "COMPLETED", "2026-01-01 10:00:00", {"city": "Austin", "state": "TX"}),
            ("ORD001", "U001", 150.0, "COMPLETED", "2026-01-01 11:00:00", {"city": "Austin", "state": "TX"}),  # Duplicate, newer
            ("ORD001", "U001", 120.0, "COMPLETED", "2026-01-01 09:00:00", {"city": "Austin", "state": "TX"}),  # Duplicate, older
        ]
        
        df_test = self.spark.createDataFrame(
            test_data,
            ["order_id", "user_id", "amount", "status", "order_timestamp_str", "shipping_address"]
        ).withColumn(
            "order_timestamp", F.col("order_timestamp_str").cast("timestamp")
        )
        
        # Apply deduplication
        df_dedup, removed = _01_silver_transformation.deduplicate_records(df_test, logger)
        
        self.assertEqual(removed, 2, "Should remove 2 duplicate records")
        self.assertEqual(df_dedup.count(), 1, "Should have 1 unique record")
        
        # Verify it kept the latest (highest amount)
        latest_amount = df_dedup.select("amount").collect()[0][0]
        self.assertEqual(latest_amount, 150.0, "Should keep latest record (150.0)")


if __name__ == "__main__":
    unittest.main()
