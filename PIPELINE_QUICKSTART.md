# Quick Start: Medallion Pipeline

## Project Structure

```
DBT-DATABRICKS-streaming-pipes/
├── .github/workflows/
│   └── medallion-pipeline.yml          # GitHub Actions CI/CD workflow
├── scripts/
│   ├── config.py                       # Centralized configuration
│   ├── utils.py                        # Shared utilities
│   ├── 00_bronze_ingestion.py          # Step 1: Raw data ingestion
│   ├── 01_silver_transformation.py     # Step 2: Data quality & cleansing
│   ├── 02_gold_aggregation.py          # Step 3: Metrics & aggregations
│   ├── 03_lakebase_sync.py             # Step 4: Database sync
│   ├── run_pipeline.py                 # Orchestrator (all steps)
│   ├── requirements.txt                # Python dependencies
│   └── PIPELINE_README.md              # Detailed documentation
├── tests/
│   ├── test_bronze.py                  # Bronze layer tests
│   ├── test_silver.py                  # Silver layer tests
│   └── test_gold.py                    # Gold layer tests
```

---

## Setup (5 minutes)

### 1. Install Dependencies

```bash
cd scripts/
pip install -r requirements.txt
```

### 2. Verify Configuration

Check `scripts/config.py` for paths and thresholds:
```python
BASE_PATH = "/tmp/databricks_lab_e2e"
VIP_SPEND_THRESHOLD = 500.0
```

---

## Run the Pipeline

### Option A: Run Individual Layers (for development)

Each layer is independently executable:

```bash
cd scripts/

# Layer 1: Ingest raw orders
python 00_bronze_ingestion.py

# Layer 2: Data quality & deduplication
python 01_silver_transformation.py

# Layer 3: Calculate metrics & aggregations
python 02_gold_aggregation.py

# Layer 4: Sync to LakeBase (dry-run mode)
python 03_lakebase_sync.py
```

**Output**: Check logs and metrics for each layer

### Option B: Run End-to-End (recommended)

```bash
cd scripts/
python run_pipeline.py
```

**Output**:
```
================================================================================
MEDALLION PIPELINE ORCHESTRATOR
================================================================================
Pipeline Start Time: 2026-08-20 10:30:00

[1/4] Executing Bronze Layer Ingestion...
✅ Bronze layer complete: /tmp/databricks_lab_e2e/bronze_orders

[2/4] Executing Silver Layer Transformation...
✅ Silver layer complete: /tmp/databricks_lab_e2e/silver_orders

[3/4] Executing Gold Layer Aggregation...
✅ Gold layer complete: /tmp/databricks_lab_e2e/gold_customer_metrics

[4/4] Executing LakeBase Operational Sync...
✅ LakeBase sync complete: SIMULATED

================================================================================
PIPELINE EXECUTION SUMMARY
================================================================================
Status: SUCCESS
Duration: 10.02 seconds
✅ All steps completed successfully!
```

---

## Run Tests

```bash
cd tests/
pytest -v --cov=scripts

# Or individual test file:
pytest test_bronze.py -v
pytest test_silver.py -v
pytest test_gold.py -v
```

---

## Enable GitHub Actions CI/CD

### 1. Add GitHub Secrets

Go to **Settings → Secrets and variables → Actions** and add:

```
DATABRICKS_HOST           = https://your-workspace.cloud.databricks.com/
DATABRICKS_TOKEN          = dapi...
DATABRICKS_CLUSTER_ID     = 1234-567890-abc
LAKEBASE_HOST             = lakebase.databricks.com
LAKEBASE_USER             = lakebase_user
LAKEBASE_PASSWORD         = your_password
```

### 2. Push to GitHub

```bash
git add scripts/ .github/workflows/
git commit -m "Add modular medallion pipeline"
git push origin main
```

### 3. Monitor Workflow

- Go to **Actions** tab
- Click **Medallion Pipeline**
- Watch jobs execute:
  - ✅ Bronze Ingestion
  - ✅ Silver Transformation
  - ✅ Gold Aggregation
  - ✅ LakeBase Sync

---

## What Each Layer Does

### 🔵 Bronze Layer (`00_bronze_ingestion.py`)
- **Input**: Raw orders data (mock data in demo)
- **Process**: Load raw data, add ingestion metadata
- **Output**: `/tmp/databricks_lab_e2e/bronze_orders`
- **Records**: 7 raw orders (includes duplicates & invalid)
- **Example**:
  ```
  ORD101 | U1001 | $150.50 | COMPLETED | Austin, TX
  ORD102 | U1002 | $200.00 | PENDING   | Dallas, TX
  ...
  ```

### 🟢 Silver Layer (`01_silver_transformation.py`)
- **Input**: Bronze raw data
- **Process**: 
  - Remove null amounts & invalid statuses
  - Deduplicate by order_id (keep latest)
  - Flatten nested structs (shipping_address)
- **Output**: `/tmp/databricks_lab_e2e/silver_orders`
- **Records**: 5 clean, deduplicated orders
- **Quality Rate**: ~71% (2 records removed)

### 🟡 Gold Layer (`02_gold_aggregation.py`)
- **Input**: Silver clean data
- **Process**:
  - Calculate lifetime cumulative spend per user
  - Calculate YTD (year-to-date) spend
  - Identify VIP customers (spend >= $500)
- **Output**: `/tmp/databricks_lab_e2e/gold_customer_metrics`
- **Records**: 4 unique customers with aggregated metrics
- **Example**:
  ```
  U1001 | $900.50  | 2 orders | VIP: Yes
  U1002 | $500.00  | 2 orders | VIP: Yes
  U1003 | $0.00    | 0 orders | VIP: No
  U1004 | $1200.00 | 1 order  | VIP: Yes
  ```

### 🟣 LakeBase Sync (`03_lakebase_sync.py`)
- **Input**: Gold customer metrics
- **Process**: Write to PostgreSQL operational database
- **Output**: `operational_db.gold_user_metrics_lakebase`
- **Mode**: OVERWRITE (or MERGE in production)
- **Default**: Dry-run mode (no actual writes)
- **Enable Production**: Set `DRY_RUN=false` in workflow

---

## Key Features

✅ **Modular**: Each layer is independently executable  
✅ **Testable**: Unit tests for each layer  
✅ **Observable**: Detailed logging and metrics  
✅ **CI/CD Ready**: GitHub Actions workflow included  
✅ **Error Handling**: Validates data quality at each step  
✅ **Configurable**: Centralized config for all paths/thresholds  
✅ **Production Safe**: Dry-run mode by default for database writes  

---

## Metrics & Monitoring

Each layer logs structured metrics:

```json
{
  "layer": "silver",
  "input_records": 7,
  "output_records": 5,
  "quality_filters_removed": 2,
  "duplicates_removed": 1,
  "data_quality_rate": 71.43,
  "processing_timestamp": "2026-08-20T10:30:05"
}
```

View logs:
```bash
grep "METRICS:" *.log  # Extract all metrics
tail -f run_pipeline.py  # Stream live execution
```

---

## Customization

### Change VIP Threshold

Edit `scripts/config.py`:
```python
VIP_SPEND_THRESHOLD = 1000.0  # Changed from 500.0
```

### Add Custom Transformation

1. Create `scripts/04_custom_layer.py`
2. Import utils: `from utils import setup_logger`
3. Implement transformation
4. Add to `run_pipeline.py`

### Connect Real Data Source

Replace `generate_raw_orders_data()` in `00_bronze_ingestion.py`:
```python
# Instead of mock data:
df_raw = spark.read.kafka(...).load()  # Kafka
# or
df_raw = spark.read.parquet("s3://bucket/orders")  # S3
```

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Import errors | Run `pip install -r requirements.txt` |
| Path not found | Check `BASE_PATH` in `config.py` |
| No data in output | Verify previous layer executed successfully |
| Workflow not running | Check GitHub Actions is enabled in repo settings |

---

## Next: Deploy to Production

1. **Connect to Databricks cluster**
   - Update `BRONZE_PATH` to Unity Catalog path
   - Modify `00_bronze_ingestion.py` to read from real sources

2. **Enable LakeBase writes**
   - Set `DRY_RUN=false` in workflow
   - Configure JDBC credentials

3. **Add monitoring/alerting**
   - Integrate with PagerDuty/Datadog
   - Create alerts for failed runs

4. **Schedule runs**
   - Set cron schedule in `.github/workflows/medallion-pipeline.yml`
   - Default: Daily at 2 AM UTC

---

**Questions?** Check `scripts/PIPELINE_README.md` for detailed documentation.
