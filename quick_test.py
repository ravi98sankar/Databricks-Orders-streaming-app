#!/usr/bin/env python3
"""Quick Databricks Connect diagnostic."""

import os
import sys
import signal

def timeout_handler(signum, frame):
    print("\n⏱️  Connection attempt timed out (5 seconds)")
    sys.exit(1)

# Set 5-second timeout
signal.signal(signal.SIGALRM, timeout_handler)
signal.alarm(5)

try:
    # Load env vars manually
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                key, val = line.split('=', 1)
                os.environ[key] = val
    
    from databricks.connect import DatabricksSession
    
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
    
    print(f"Testing connection to: {host}")
    print(f"Cluster ID: {cluster_id}")
    print("Connecting...")
    
    spark = DatabricksSession.builder \
        .host(host) \
        .token(token) \
        .clusterId(cluster_id) \
        .getOrCreate()
    
    signal.alarm(0)  # Cancel timeout
    
    print("✅ Connected successfully!")
    print(f"Spark version: {spark.version}")
    
except Exception as e:
    signal.alarm(0)
    print(f"❌ Connection failed: {e}")
    sys.exit(1)
