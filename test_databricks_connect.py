#!/usr/bin/env python3
"""
Test Databricks Connect connectivity.

Setup:
  1. Copy .env.example to .env and fill in credentials (optional)
  2. Or set environment variables:
     - DATABRICKS_HOST
     - DATABRICKS_TOKEN
     - DATABRICKS_CLUSTER_ID
  3. Run: python test_databricks_connect.py
"""

import os
import sys
from pathlib import Path

# Load environment variables from .env file (optional)
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        print(f"✓ Loaded .env from {env_path}")
except ImportError:
    pass  # python-dotenv not required


def test_databricks_connect():
    """Test connection to Databricks using Databricks Connect."""
    
    print("\n" + "="*70)
    print("DATABRICKS CONNECT TEST")
    print("="*70 + "\n")
    
    # Get required credentials from environment
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
    
    # Check for required env vars
    missing = []
    if not host:
        missing.append("DATABRICKS_HOST")
    if not token:
        missing.append("DATABRICKS_TOKEN")
    if not cluster_id:
        missing.append("DATABRICKS_CLUSTER_ID")
    
    if missing:
        print(f"❌ Missing required environment variables:")
        for var in missing:
            print(f"   - {var}")
        print("\n   Please set these in your .env file or environment.")
        return False
    
    print(f"Configuration loaded:")
    print(f"  Host: {host}")
    print(f"  Token: {token[:20]}..." if len(token) > 20 else f"  Token: {token}")
    print(f"  Cluster ID: {cluster_id}")
    print()
    
    # Attempt import
    try:
        from databricks.connect import DatabricksSession
        print("✓ Successfully imported DatabricksSession")
    except ImportError as e:
        print(f"❌ Failed to import DatabricksSession: {e}")
        print("   Install databricks-connect with: pip install 'databricks-connect==13.3.13'")
        return False
    
    # Attempt connection
    print("\nAttempting to connect to Databricks...\n")
    try:
        spark = DatabricksSession.builder \
            .host(host) \
            .token(token) \
            .clusterId(cluster_id) \
            .getOrCreate()
        
        print("✓ Connected to Databricks successfully!")
        
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        print("\nTroubleshooting:")
        print("  1. Verify DATABRICKS_HOST is correct (should start with https://)")
        print("  2. Verify DATABRICKS_TOKEN is valid and has not expired")
        print("  3. Verify DATABRICKS_CLUSTER_ID exists and is running")
        print("  4. Check network connectivity to your Databricks workspace")
        return False
    
    # Test basic Spark operations
    print("\nRunning test queries...\n")
    
    try:
        # Test 1: Simple range
        print("Test 1: spark.range(5).collect()")
        result = spark.range(5).collect()
        print(f"  Result: {result}")
        print("  ✓ PASSED")
        
    except Exception as e:
        print(f"  ❌ FAILED: {e}")
        return False
    
    try:
        # Test 2: Create a test DataFrame
        print("\nTest 2: Create and display a test DataFrame")
        df = spark.createDataFrame(
            [
                ("Alice", 25),
                ("Bob", 30),
                ("Charlie", 35),
            ],
            ["name", "age"]
        )
        print("  DataFrame created with schema:")
        df.printSchema()
        print("  Data:")
        df.show()
        print("  ✓ PASSED")
        
    except Exception as e:
        print(f"  ❌ FAILED: {e}")
        return False
    
    try:
        # Test 3: SQL query
        print("\nTest 3: Register table and run SQL")
        df.createOrReplaceTempView("test_table")
        result_df = spark.sql("SELECT * FROM test_table WHERE age > 25")
        print("  Query: SELECT * FROM test_table WHERE age > 25")
        result_df.show()
        print("  ✓ PASSED")
        
    except Exception as e:
        print(f"  ❌ FAILED: {e}")
        return False
    
    print("\n" + "="*70)
    print("✓ ALL TESTS PASSED!")
    print("="*70 + "\n")
    print("Your Databricks Connect setup is working correctly.")
    print("You can now use this connection in your dbt models and Python scripts.\n")
    
    return True


if __name__ == "__main__":
    success = test_databricks_connect()
    sys.exit(0 if success else 1)
