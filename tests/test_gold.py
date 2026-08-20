"""
Unit tests for Gold Layer aggregation.
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from scripts.scripts import _02_gold_aggregation
from config import GOLD_PATH, VIP_SPEND_THRESHOLD


class TestGoldAggregation(unittest.TestCase):
    """Test cases for Gold layer."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize Spark session for tests."""
        cls.spark = SparkSession.builder.appName("GoldTests").getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Stop Spark session."""
        cls.spark.stop()
    
    def test_cumulative_metrics_calculation(self):
        """Test that cumulative spend metrics are calculated correctly."""
        from utils import setup_logger
        
        logger = setup_logger("GoldTest")
        
        # Create test data with known spend values
        test_data = [
            ("U001", "ORD001", 100.0, "2026-01-01 10:00:00"),
            ("U001", "ORD002", 200.0, "2026-01-02 10:00:00"),
            ("U001", "ORD003", 300.0, "2026-01-03 10:00:00"),
            ("U002", "ORD004", 150.0, "2026-01-01 10:00:00"),
        ]
        
        df_test = self.spark.createDataFrame(
            test_data,
            ["user_id", "order_id", "amount", "order_timestamp"]
        ).withColumn(
            "order_timestamp", F.col("order_timestamp").cast("timestamp")
        )
        
        # Calculate metrics
        df_metrics = _02_gold_aggregation.calculate_cumulative_metrics(df_test, logger)
        
        # Verify U001 lifetime spend is 600 (100 + 200 + 300)
        u001_spending = df_metrics.filter(F.col("user_id") == "U001").select("lifetime_cumulative_spend").collect()
        self.assertEqual(len(u001_spending), 3, "Should have 3 records for U001")
        self.assertEqual(u001_spending[2][0], 600.0, "Last record should have cumulative spend of 600")
    
    def test_vip_identification(self):
        """Test that VIP customers are identified correctly."""
        from utils import setup_logger
        
        logger = setup_logger("GoldTest")
        
        # Create test data
        test_data = [
            ("U001", "ORD001", 600.0, "2026-01-01 10:00:00"),  # VIP (>= threshold)
            ("U002", "ORD002", 400.0, "2026-01-01 10:00:00"),  # Not VIP
        ]
        
        df_test = self.spark.createDataFrame(
            test_data,
            ["user_id", "order_id", "amount", "order_timestamp"]
        ).withColumn(
            "order_timestamp", F.col("order_timestamp").cast("timestamp")
        )
        
        # Calculate metrics
        df_metrics = _02_gold_aggregation.calculate_cumulative_metrics(df_test, logger)
        
        # Check VIP status
        vip_count = df_metrics.filter(F.col("is_vip_customer") == True).count()
        self.assertEqual(vip_count, 1, "Should identify 1 VIP customer")


if __name__ == "__main__":
    unittest.main()
