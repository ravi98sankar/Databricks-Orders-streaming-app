# Medallion Pipeline - End-to-End Architecture

## Overview

This is a **production-ready, modular, GitHub Actions-based** end-to-end medallion architecture (Bronze → Silver → Gold → LakeBase) that processes orders data through a complete data pipeline.

### Pipeline Stages

```
Raw Data (Kafka/S3/Batch)
        ↓
    [BRONZE] - Raw Ingestion (config: .env file inputs)
        ↓ (ingestion_metadata added)
    [SILVER] - Data Quality, Deduplication, Flattening
        ↓ (clean, validated data)
    [GOLD]   - Business Metrics, Aggregations, VIP Identification
        ↓ (customer-centric analytics)
    [LakeBase] - Operational Database Sync (PostgreSQL)
```

---

## Architecture Components

### 1. **Scripts** (`/scripts/`)

Each layer has a dedicated, independently executable module:

#### `config.py`
- Centralized configuration (paths, thresholds, credentials)
- Environment variable substitution for CI/CD
- Database connection details

#### `utils.py`
- Shared utilities: logging, metrics, validation, benchmarking
- Error handling and data quality checks
- Reusable functions across all layers

#### `00_bronze_ingestion.py`
- **Purpose**: Ingest raw orders data
- **Input**: Mock data (or Kafka/S3 in production)
- **Output**: Delta Lake table at `/tmp/databricks_lab_e2e/bronze_orders`
- **Features**:
  - Adds `_ingested_at` timestamp
  - Adds `_ingestion_source` metadata
  - Handles timestamp parsing

#### `01_silver_transformation.py`
- **Purpose**: Data quality, deduplication, cleansing
- **Input**: Bronze raw data
- **Output**: Clean, deduplicated table at `/tmp/databricks_lab_e2e/silver_orders`
- **Transformations**:
  - Filters invalid records (null amounts, cancelled orders)
  - Deduplicates by order_id (keeps latest)
  - Flattens nested structs (shipping_address)
  - Adds `_processed_at` audit columns

#### `02_gold_aggregation.py`
- **Purpose**: Business metrics and customer analytics
- **Input**: Silver cleaned data
- **Output**: Customer summary at `/tmp/databricks_lab_e2e/gold_customer_metrics`
- **Metrics**:
  - Lifetime cumulative spend per customer
  - YTD (year-to-date) cumulative spend
  - VIP identification (spend >= $500)
  - Order counts, timestamps, min/max amounts

#### `03_lakebase_sync.py`
- **Purpose**: Sync Gold layer to external operational database
- **Input**: Gold customer metrics
- **Output**: PostgreSQL table `gold_user_metrics_lakebase`
- **Features**:
  - JDBC-based write to LakeBase
  - Dry-run mode for testing (default)
  - Configurable OVERWRITE vs. MERGE strategies
  - Connectivity validation

#### `run_pipeline.py`
- **Orchestrator**: Executes the complete pipeline end-to-end
- Handles error propagation and partial failures
- Generates pipeline execution report
- Provides exit codes for CI/CD integration

---

### 2. **GitHub Actions Workflow** (`.github/workflows/medallion-pipeline.yml`)

**Modular, parallel-ready CI/CD pipeline:**

```
Trigger (push/PR/schedule/manual)
    ↓
[Job 1: Bronze] ────────────┐
    ↓                        │
[Job 2: Silver] ◄───────────┘ (depends_on: Bronze)
    ↓
[Job 3: Gold] ◄───────────────────── (depends_on: Silver)
    ↓
[Job 4: LakeBase] ◄────────────────── (depends_on: Gold)
    ↓
[Summary Report] ◄────────────────── (depends_on: All)
```

**Job Details:**

| Job | Task | Depends On | Actions |
|-----|------|-----------|---------|
| **Bronze** | Raw data ingestion | - | Run ingestion, validate output |
| **Silver** | Data quality & dedup | Bronze | Run transformation, validate schema |
| **Gold** | Aggregations & metrics | Silver | Run aggregation, generate report |
| **LakeBase** | DB sync | Gold | Run sync (dry-run by default) |
| **Summary** | Final report | All | Generate execution summary |

**Features:**
- Parallel execution where possible (within dependency constraints)
- Dry-run mode for LakeBase sync (disable with `DRY_RUN=false`)
- Artifact collection for logs and metrics
- GitHub Step Summary for visibility
- Scheduled runs (daily at 2 AM UTC)
- Manual workflow dispatch

---

### 3. **Test Suite** (`/tests/`)

Comprehensive unit tests for data quality validation:

- **test_bronze.py**: Ingestion and data generation
- **test_silver.py**: Quality filters, deduplication, flattening
- **test_gold.py**: Cumulative metrics, VIP identification

Run with:
```bash
pytest tests/ -v --cov=scripts
```

---

## Configuration

### Environment Variables (GitHub Secrets)

```yaml
DATABRICKS_HOST       # e.g., https://adb-xxxxx.cloud.databricks.com/
DATABRICKS_TOKEN      # PAT token for authentication
DATABRICKS_CLUSTER_ID # Cluster ID for compute

LAKEBASE_HOST         # PostgreSQL host (e.g., lakebase.databricks.com)
LAKEBASE_USER         # Database user
LAKEBASE_PASSWORD     # Database password
```

### Local Config (`scripts/config.py`)

```python
BASE_PATH = "/tmp/databricks_lab_e2e"
BRONZE_PATH = f"{BASE_PATH}/bronze_orders"
SILVER_PATH = f"{BASE_PATH}/silver_orders"
GOLD_PATH = f"{BASE_PATH}/gold_customer_metrics"

VIP_SPEND_THRESHOLD = 500.0
MIN_VALID_AMOUNT = 0.01
```

---

## Execution Methods

### 1. **Local Development** (Single script)

```bash
cd scripts/

# Run individual layers
python 00_bronze_ingestion.py
python 01_silver_transformation.py
python 02_gold_aggregation.py
python 03_lakebase_sync.py  # Dry-run by default
```

### 2. **Local End-to-End**

```bash
cd scripts/
python run_pipeline.py
```

### 3. **GitHub Actions (Recommended for production)**

Push to `main` branch or manual trigger:
```bash
git push origin main
# OR via UI: Actions → Medallion Pipeline → Run Workflow
```

---

## Data Quality Metrics

Each stage logs detailed metrics:

### Bronze Layer
```json
{
  "layer": "bronze",
  "total_records": 7,
  "ingestion_timestamp": "2026-08-20T10:30:00"
}
```

### Silver Layer
```json
{
  "layer": "silver",
  "input_records": 7,
  "output_records": 5,
  "quality_filters_removed": 2,
  "duplicates_removed": 1,
  "data_quality_rate": 71.43
}
```

### Gold Layer
```json
{
  "layer": "gold",
  "input_records": 5,
  "output_records": 4,
  "unique_customers": 4,
  "vip_customers": 2,
  "avg_lifetime_spend": 362.50
}
```

### LakeBase Sync
```json
{
  "layer": "lakebase_sync",
  "input_records": 4,
  "target_table": "operational_db.gold_user_metrics_lakebase",
  "sync_mode": "OVERWRITE",
  "sync_status": "SIMULATED"
}
```

---

## Error Handling

- **Step Failure**: Each job validates output before proceeding
- **Partial Failures**: Subsequent jobs halt; logs preserved
- **Retry Logic**: Configure in workflow for transient failures
- **Dry-Run Mode**: LakeBase writes are simulated by default for safety

---

## Example Workflow Run

**Trigger**: Push to `main`

```
✅ Job 1: Bronze Ingestion (2.5s)
   - Generated 7 raw orders
   - Written to /tmp/databricks_lab_e2e/bronze_orders

✅ Job 2: Silver Transformation (3.2s)
   - Input: 7 records
   - Output: 5 records (removed 2 invalid)
   - Deduplicated: 1 duplicate removed
   - Quality rate: 71.43%

✅ Job 3: Gold Aggregation (2.8s)
   - Unique customers: 4
   - VIP customers: 2 (50%)
   - Avg lifetime spend: $362.50

✅ Job 4: LakeBase Sync (1.5s)
   - Dry-run mode: SIMULATED
   - Target: operational_db.gold_user_metrics_lakebase
   - Records staged: 4

✅ Pipeline Summary
   - Total duration: 10.0s
   - Status: SUCCESS
   - Artifacts: 4 metric files
```

---

## Extending the Pipeline

### Add a New Transformation Layer

1. Create `04_custom_layer.py` in `/scripts/`
2. Implement transformation logic
3. Add job to `.github/workflows/medallion-pipeline.yml`
4. Set `needs: [previous-job]`

### Connect to Different Data Source

1. Modify `generate_raw_orders_data()` in `00_bronze_ingestion.py`
2. Add Kafka/S3/API connector code
3. Update `BRONZE_PATH` in `config.py` if needed

### Enable Production LakeBase Writes

Set `DRY_RUN: 'false'` in workflow:

```yaml
- name: Run LakeBase Sync
  env:
    DRY_RUN: 'false'  # Enable actual writes
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Bronze layer fails | Check `config.py` paths and permissions |
| Silver dedup not working | Verify `order_timestamp` is properly cast |
| Gold VIP threshold off | Adjust `VIP_SPEND_THRESHOLD` in `config.py` |
| LakeBase connection error | Verify secrets and network connectivity |
| Workflow stuck | Check GitHub Actions runner logs |

---

## Next Steps

1. ✅ **Test locally**: `python 00_bronze_ingestion.py`
2. ✅ **Commit to repo**: Push to GitHub
3. ✅ **Enable Actions**: Go to Actions tab, enable workflows
4. ✅ **Set secrets**: Add `DATABRICKS_*` and `LAKEBASE_*` secrets
5. ✅ **Monitor runs**: View workflow execution and logs
6. ✅ **Scale**: Deploy to production Databricks cluster

---

**Created**: August 20, 2026  
**Last Updated**: August 20, 2026  
**Maintainer**: Databricks dbt Team
