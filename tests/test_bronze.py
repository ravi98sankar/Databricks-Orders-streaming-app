"""
Unit tests for Bronze Layer ingestion.
"""

import unittest
from pyspark.sql import SparkSession
from scripts.scripts import _00_bronze_ingestion
from config import BRONZE_PATH


class TestBronzeIngestion(unittest.TestCase):
    """Test cases for Bronze layer."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize Spark session for tests."""
        cls.spark = SparkSession.builder.appName("BronzeTests").getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Stop Spark session."""
        cls.spark.stop()
    
    def test_raw_data_generation(self):
        """Test that raw data is generated correctly."""
        raw_data = _00_bronze_ingestion.generate_raw_orders_data()
        
        self.assertEqual(len(raw_data), 7, "Should generate 7 records")
        
        # Verify first record structure
        first_record = raw_data[0]
        self.assertEqual(len(first_record), 6, "Each record should have 6 fields")
    
    def test_spark_session_creation(self):
        """Test Spark session initialization."""
        spark = _00_bronze_ingestion.create_spark_session()
        
        self.assertIsNotNone(spark, "Spark session should be created")
        self.assertIsNotNone(spark.sparkContext, "Spark context should be available")
    
    def test_ingestion_produces_output(self):
        """Test that ingestion produces non-empty output."""
        from utils import setup_logger
        
        logger = setup_logger("BronzeTest")
        
        bronze_path = _00_bronze_ingestion.ingest_raw_orders(self.spark, logger)
        
        self.assertEqual(bronze_path, BRONZE_PATH, "Should return correct path")
        
        # Verify data was written
        df_verify = self.spark.read.format("delta").load(BRONZE_PATH)
        self.assertGreater(df_verify.count(), 0, "Should have records in Bronze")


if __name__ == "__main__":
    unittest.main()
