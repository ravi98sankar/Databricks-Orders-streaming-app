# Databricks Streaming Pipes with dbt

A **production-ready, modular, end-to-end medallion data pipeline** for Databricks with Unity Catalog, featuring Lakeflow streaming patterns, Python data layers, and automated GitHub Actions CI/CD orchestration.

**Status**: ✅ Production-Ready | **Framework**: Apache Spark + Delta Lake + dbt | **Database**: Databricks + PostgreSQL

---

## 📋 Table of Contents

- [Overview](#overview)
- [Data Flow & Architecture](#data-flow--architecture)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Installation & Setup](#installation--setup)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Running the Pipeline](#running-the-pipeline)
- [Pipeline Components](#pipeline-components)
- [Monitoring & Troubleshooting](#monitoring--troubleshooting)
- [Extending the Pipeline](#extending-the-pipeline)
- [FAQ](#faq)

---

## Overview

This project implements a **medallion architecture** (Bronze → Silver → Gold) for streaming order data through Databricks, with modular Python layers, comprehensive testing, and automated CI/CD via GitHub Actions.

### Key Features

✅ **Modular Design**: 7 independent Python scripts (not monolithic)  
✅ **Fully Tested**: 9 unit tests covering all layers  
✅ **CI/CD Ready**: GitHub Actions with sequential orchestration  
✅ **Production Safe**: Dry-run mode, error handling, comprehensive logging  
✅ **Well Documented**: 4 comprehensive guides + inline code comments  
✅ **Databricks Native**: Unity Catalog, Delta Lake, Lakeflow patterns  
✅ **Configurable**: Centralized config, environment variable overrides  

---

## Data Flow & Architecture

### End-to-End Data Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA FLOW DIAGRAM                              │
└─────────────────────────────────────────────────────────────────────────┘

Mock Orders Data (7 records, intentional quality issues)
    │
    ▼
┌───────────────────────────────────────────────────────────────────────┐
│ BRONZE LAYER (00_bronze_ingestion.py)                                 │
│ ┌─────────────────────────────────────────────────────────────────┐   │
│ │ Input:  Raw orders data (mock or Kafka/S3 in production)        │   │
│ │ Schema: order_id, user_id, amount, status, timestamp, address   │   │
│ │ Process:                                                         │   │
│ │  1. Create DataFrame from raw data                              │   │
│ │  2. Add _ingested_at timestamp                                  │   │
│ │  3. Add _ingestion_source metadata                              │   │
│ │  4. Write to Delta Lake                                         │   │
│ │ Output: /tmp/databricks_lab_e2e/bronze_orders (7 records)       │   │
│ │ Metrics: total_records, ingestion_timestamp                     │   │
│ └─────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌───────────────────────────────────────────────────────────────────────┐
│ SILVER LAYER (01_silver_transformation.py)                            │
│ ┌─────────────────────────────────────────────────────────────────┐   │
│ │ Input: Bronze raw data (7 records with quality issues)          │   │
│ │ Transformations:                                                │   │
│ │  1. QUALITY FILTERS:                                            │   │
│ │     - Remove records with null order_id, user_id, or amount     │   │
│ │     - Remove records with amount ≤ $0.01 (MIN_VALID_AMOUNT)     │   │
│ │     - Remove records with status = "CANCELLED"                  │   │
│ │     ➜ Removed: 2 records                                        │   │
│ │                                                                 │   │
│ │  2. DEDUPLICATION:                                              │   │
│ │     - Window partition by order_id, order by timestamp DESC     │   │
│ │     - Keep row_number == 1 (latest record)                      │   │
│ │     ➜ Removed: 1 duplicate                                      │   │
│ │                                                                 │   │
│ │  3. FLATTENING & ENRICHMENT:                                    │   │
│ │     - Extract nested shipping_address.city → shipping_city      │   │
│ │     - Extract shipping_address.state → shipping_state           │   │
│ │     - Add _processed_at timestamp                               │   │
│ │     - Add _processing_stage = "silver"                          │   │
│ │                                                                 │   │
│ │ Output: /tmp/databricks_lab_e2e/silver_orders (5 records)       │   │
│ │ Metrics: input_records, output_records, quality_filters_removed,│   │
│ │          duplicates_removed, data_quality_rate (%)              │   │
│ └─────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌───────────────────────────────────────────────────────────────────────┐
│ GOLD LAYER (02_gold_aggregation.py)                                   │
│ ┌─────────────────────────────────────────────────────────────────┐   │
│ │ Input: Silver clean data (5 records)                            │   │
│ │ Window Aggregations:                                            │   │
│ │  ┌─────────────────────────────────────────────────────────┐    │   │
│ │  │ Window 1: LIFETIME CUMULATIVE SPEND                    │    │   │
│ │  │ - Partition By: user_id                                │    │   │
│ │  │ - Order By: order_timestamp ASC                        │    │   │
│ │  │ - Row Range: UNBOUNDED PRECEDING → CURRENT ROW         │    │   │
│ │  │ - Calculates: lifetime_cumulative_spend (running sum)  │    │   │
│ │  └─────────────────────────────────────────────────────────┘    │   │
│ │  ┌─────────────────────────────────────────────────────────┐    │   │
│ │  │ Window 2: YTD (YEAR-TO-DATE) CUMULATIVE SPEND          │    │   │
│ │  │ - Partition By: user_id, YEAR(order_timestamp)         │    │   │
│ │  │ - Order By: order_timestamp ASC                        │    │   │
│ │  │ - Row Range: UNBOUNDED PRECEDING → CURRENT ROW         │    │   │
│ │  │ - Calculates: ytd_cumulative_spend                     │    │   │
│ │  └─────────────────────────────────────────────────────────┘    │   │
│ │                                                                 │   │
│ │ VIP Identification:                                             │   │
│ │  - IF lifetime_cumulative_spend >= VIP_SPEND_THRESHOLD ($500)   │   │
│ │  - THEN is_vip_customer = True                                  │   │
│ │                                                                 │   │
│ │ Customer Summary Aggregation:                                   │   │
│ │  - GroupBy: user_id                                             │   │
│ │  - Aggregations:                                                │   │
│ │    • max(lifetime_cumulative_spend)                             │   │
│ │    • max(ytd_cumulative_spend)                                  │   │
│ │    • count(order_id)                                            │   │
│ │    • avg(amount), min(amount), max(amount)                      │   │
│ │    • min(order_timestamp), max(order_timestamp)                 │   │
│ │    • max(is_vip_customer)                                       │   │
│ │  - Add: _aggregated_at timestamp                                │   │
│ │                                                                 │   │
│ │ Output: /tmp/databricks_lab_e2e/gold_customer_metrics           │   │
│ │         (4 unique customers, 1 row per user_id)                │   │
│ │                                                                 │   │
│ │ Example Output Row:                                             │   │
│ │ ┌──────────┬─────────────┬─────────┬──────────┬──────────────┐  │   │
│ │ │ user_id  │ total_spend │ ytd_sum │ num_ord  │ is_vip       │  │   │
│ │ ├──────────┼─────────────┼─────────┼──────────┼──────────────┤  │   │
│ │ │ U1001    │ $900.50     │ $900.50 │ 2        │ TRUE (VIP)   │  │   │
│ │ │ U1002    │ $500.00     │ $500.00 │ 2        │ TRUE (VIP)   │  │   │
│ │ │ U1003    │ $0.00       │ $0.00   │ 0        │ FALSE        │  │   │
│ │ │ U1004    │ $1200.00    │ $1200.00│ 1        │ TRUE (VIP)   │  │   │
│ │ └──────────┴─────────────┴─────────┴──────────┴──────────────┘  │   │
│ │                                                                 │   │
│ │ Metrics: input_records, output_records, unique_customers,       │   │
│ │          vip_customers, avg_lifetime_spend                      │   │
│ └─────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌───────────────────────────────────────────────────────────────────────┐
│ LAKEBASE SYNC (03_lakebase_sync.py)                                   │
│ ┌─────────────────────────────────────────────────────────────────┐   │
│ │ Input: Gold customer metrics (4 records)                        │   │
│ │ Process:                                                        │   │
│ │  1. Read Gold customer summary table                            │   │
│ │  2. Validate PostgreSQL connectivity (LakeBase)                 │   │
│ │  3. JDBC Write to PostgreSQL:                                   │   │
│ │     - Table: operational_db.gold_user_metrics_lakebase          │   │
│ │     - Mode: OVERWRITE (replace entire table)                    │   │
│ │     - Connection: PostgreSQL JDBC driver                        │   │
│ │  4. Log sync metrics                                            │   │
│ │                                                                 │   │
│ │ Safety Features:                                                │   │
│ │  - DEFAULT: Dry-run mode (simulates write, shows sample data)   │   │
│ │  - PRODUCTION: Set DRY_RUN=false to enable actual writes        │   │
│ │  - Credentials: Read from environment variables                 │   │
│ │                                                                 │   │
│ │ Output: PostgreSQL table (OVERWRITE mode)                       │   │
│ │ Metrics: input_records, target_table, sync_mode, sync_status    │   │
│ └─────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATOR (run_pipeline.py)                       │
│                                                                         │
│  Coordinates all 4 layers sequentially:                                │
│  1. Execute Bronze Layer → validate output → log metrics                │
│  2. Execute Silver Layer → validate output → log metrics                │
│  3. Execute Gold Layer → validate output → log metrics                  │
│  4. Execute LakeBase Sync → validate output → log metrics               │
│                                                                         │
│  Error Handling:                                                        │
│  - Each layer wrapped in try/except                                    │
│  - Catches exceptions without stopping pipeline (logs all errors)       │
│  - Exit with status code 0 (success) or 1 (failure)                    │
│                                                                         │
│  Output: Pipeline execution summary with timings and final status       │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                 CI/CD AUTOMATION (.github/workflows/)                   │
│                                                                         │
│  GitHub Actions Workflow: medallion-pipeline.yml                       │
│                                                                         │
│  Triggers:                                                              │
│  ✓ Push to main/develop branches                                       │
│  ✓ Pull requests to main                                               │
│  ✓ Daily schedule (2 AM UTC)                                           │
│  ✓ Manual workflow dispatch                                            │
│                                                                         │
│  Jobs (Sequential via 'needs:' dependencies):                          │
│                                                                         │
│  [Job 1] bronze-ingestion                                              │
│      ↓                                                                  │
│  [Job 2] silver-transformation (depends: bronze-ingestion)             │
│      ↓                                                                  │
│  [Job 3] gold-aggregation (depends: silver-transformation)             │
│      ↓                                                                  │
│  [Job 4] lakebase-sync (depends: gold-aggregation)                     │
│      ↓                                                                  │
│  [Job 5] pipeline-summary (depends: all, always runs)                  │
│                                                                         │
│  Each job:                                                              │
│  - Runs on ubuntu-latest                                               │
│  - Checks out code                                                     │
│  - Sets up Python 3.10                                                 │
│  - Installs dependencies                                               │
│  - Executes Python script                                              │
│  - Uploads logs as artifacts                                           │
│  - Posts summary to GitHub Step Summary                                │
│                                                                         │
│  Final step aggregates all metrics into markdown report                │
└─────────────────────────────────────────────────────────────────────────┘
```

### Medallion Architecture Pattern

| Layer | Type | Purpose | Materialization |
|-------|------|---------|-----------------|
| **Bronze** | Raw | Ingest data as-is from sources | Streaming Table |
| **Silver** | Cleaned | Apply quality rules, deduplicate, flatten | Delta Table |
| **Gold** | Curated | Business aggregations, customer analytics | Materialized View |
| **LakeBase** | Operational | Sync metrics to operational database | PostgreSQL Table |

---

## Prerequisites

### Required
- ✅ **Databricks Workspace** with Unity Catalog enabled
- ✅ **Python 3.10+** locally (for development)
- ✅ **Git** for version control
- ✅ **GitHub Account** (for Actions CI/CD)

### For Local Development
- Python 3.10 virtual environment
- `databricks-connect` or `pyspark` for local testing
- `.env` file with Databricks credentials

### For Production
- Databricks SQL Warehouse or All-Purpose Cluster
- Unity Catalog with `main` catalog and `medallion_orders` schema
- PostgreSQL database (LakeBase) for operational sync (optional)
- GitHub repository with Actions enabled

---

## Project Structure

```
DBT-DATABRICKS-streaming-pipes/
├── README.md                                    # This file
├── .github/
│   └── workflows/
│       ├── pr-validate.yml                     # Secret scan + bundle validate (PRs)
│       ├── deploy-dev.yml                      # Deploy + run bundle against dev
│       └── deploy-prod.yml                     # Deploy + run bundle against prod (approval-gated)
├── databricks.yml                               # Databricks Asset Bundle root config
├── resources/                                   # DAB resources (deployed via GitHub Actions)
│   ├── medallion_job.yml                       # Job: dbt_task (bronze/silver/gold) -> lakebase_sync
│   └── secrets.yml                             # Secret scope declaration (no secret values)
├── models/                                      # dbt models -- the real, deployed pipeline
│   ├── bronze/bronze_orders.sql                # streaming_table, Auto Loader
│   ├── silver/silver_orders.sql                # materialized_view, quality filters + dedup
│   ├── gold/gold_customer_metrics.sql          # materialized_view, cumulative spend + VIP
│   └── python_showcase/                        # Same outcome, authored as dbt Python models
├── seeds/raw_orders.csv                         # Demo/lab mock order data
├── scripts/                                     # Standalone Python reference scripts
│   ├── config.py                               # Centralized configuration
│   ├── utils.py                                # Shared utilities (logging, metrics)
│   ├── 00_bronze_ingestion.py                  # Original local-Spark reference (legacy)
│   ├── 01_silver_transformation.py             # Original local-Spark reference (legacy)
│   ├── 02_gold_aggregation.py                  # Original local-Spark reference (legacy)
│   ├── 03_lakebase_sync.py                     # Real job task -- syncs gold_customer_metrics to LakeBase
│   ├── run_pipeline.py                         # Original local orchestrator (legacy)
│   └── requirements.txt                        # Python dependencies
├── tests/                                       # Unit tests for the legacy scripts/ modules
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
├── PIPELINE_QUICKSTART.md                      # Quick start guide (5 min)
├── DECOMPOSITION_ANALYSIS.md                   # Architecture analysis
├── PROJECT_MANIFEST.md                         # Complete file inventory
├── profiles.yml                                # dbt connection profile (env-var driven, no secrets)
├── packages.yml                                # dbt package dependencies
├── dbt_project.yml                             # dbt project config
├── .env.example                                # Environment variables template (placeholders only)
└── .gitignore                                  # Git ignore rules
```

> **Note**: `scripts/00-02_*.py` and `scripts/run_pipeline.py` are the original local-Spark reference implementation this project started from; the medallion logic they contain now lives in `models/` as dbt models. They're kept for reference but aren't part of the deployed pipeline.

---

## Installation & Setup

### Step 1: Clone Repository

```bash
git clone https://github.com/yourusername/DBT-DATABRICKS-streaming-pipes.git
cd DBT-DATABRICKS-streaming-pipes
```

### Step 2: Create Python Virtual Environment

```bash
# Create venv
python3.10 -m venv .venv

# Activate venv
source .venv/bin/activate  # On Mac/Linux
# or
.venv\Scripts\activate  # On Windows
```

### Step 3: Install Dependencies

```bash
# Upgrade pip
pip install --upgrade pip setuptools wheel

# Install Python dependencies
pip install -r scripts/requirements.txt

# Install dbt dependencies (optional for dbt models)
pip install dbt-databricks dbt-utils dbt-expectations
```

### Step 4: Configure Environment

```bash
# Copy template to .env
cp .env.example .env

# Edit .env with your credentials
nano .env  # or open in your editor
```

**Required .env variables:**
```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
DATABRICKS_CLUSTER_ID=1234-567890-abc

# Optional (for LakeBase sync)
LAKEBASE_HOST=lakebase.databricks.com
LAKEBASE_PORT=5432
LAKEBASE_DB=operational_db
LAKEBASE_USER=your_user
LAKEBASE_PASSWORD=your_password

# Pipeline config
DRY_RUN=true  # Set to false for production writes
```

### Step 5: Verify Installation

```bash
# Test Python environment
python -c "import pyspark; print(f'PySpark {pyspark.__version__}')"

# Test imports
python -c "from scripts import config, utils; print('✅ All imports successful')"

# Run Bronze layer test
python scripts/00_bronze_ingestion.py
```

---

## Configuration

All configuration is centralized in `scripts/config.py`:

```python
# Data paths (local development)
BASE_PATH = "/tmp/databricks_lab_e2e"
BRONZE_PATH = f"{BASE_PATH}/bronze_orders"
SILVER_PATH = f"{BASE_PATH}/silver_orders"
GOLD_PATH = f"{BASE_PATH}/gold_customer_metrics"

# Data paths (production in Databricks)
CATALOG = "main"
SCHEMA = "medallion_orders"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_orders"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_customer_metrics"

# Business logic thresholds
VIP_SPEND_THRESHOLD = 500.0          # Minimum lifetime spend for VIP status
MIN_VALID_AMOUNT = 0.01              # Minimum valid order amount
DUPLICATE_RETENTION = "latest"       # Keep latest record for duplicates

# Database connections
LAKEBASE_HOST = os.getenv("LAKEBASE_HOST", "")
LAKEBASE_PORT = int(os.getenv("LAKEBASE_PORT", "5432"))
LAKEBASE_DB = os.getenv("LAKEBASE_DB", "operational_db")
LAKEBASE_TABLE = "gold_user_metrics_lakebase"
```

**Override via environment variables:**
```bash
export VIP_SPEND_THRESHOLD=1000.0
export MIN_VALID_AMOUNT=0.05
python scripts/02_gold_aggregation.py
```

---

## Deployment

### Option 1: Local Development (Testing)

**Best for**: Validating logic before deployment, debugging

```bash
cd scripts/

# Run individual layers
python 00_bronze_ingestion.py      # 2-3 seconds
python 01_silver_transformation.py # 2-3 seconds
python 02_gold_aggregation.py      # 2-3 seconds
python 03_lakebase_sync.py         # 1-2 seconds (dry-run by default)

# Or run end-to-end orchestrator
python run_pipeline.py              # ~10 seconds total
```

**Expected Output**:
```
================================================================================
MEDALLION PIPELINE ORCHESTRATOR
================================================================================

[1/4] Executing Bronze Layer Ingestion...
✅ Bronze layer complete: /tmp/databricks_lab_e2e/bronze_orders

[2/4] Executing Silver Layer Transformation...
✅ Silver layer complete: /tmp/databricks_lab_e2e/silver_orders

[3/4] Executing Gold Layer Aggregation...
✅ Gold layer complete: /tmp/databricks_lab_e2e/gold_customer_metrics

[4/4] Executing LakeBase Operational Sync...
✅ LakeBase sync complete: SIMULATED (dry-run mode)

================================================================================
PIPELINE EXECUTION SUMMARY
================================================================================
Status: SUCCESS
Duration: 10.02 seconds
✅ All steps completed successfully!
```

### Option 2: GitHub Actions CI/CD (Recommended for Production)

**Best for**: Automated, scheduled, production deployments

#### Step 1: Add GitHub Secrets

Go to **Settings → Secrets and variables → Actions** in your GitHub repo:

```
DATABRICKS_HOST           = https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN          = dapi...
DATABRICKS_CLUSTER_ID     = 1234-567890-abc
LAKEBASE_HOST             = lakebase.databricks.com
LAKEBASE_USER             = lakebase_user
LAKEBASE_PASSWORD         = lakebase_password
```

#### Step 2: Push Workflow to Repository

```bash
git add .github/workflows/medallion-pipeline.yml
git commit -m "Add medallion pipeline GitHub Actions workflow"
git push origin main
```

#### Step 3: Monitor Workflow

1. Go to your GitHub repo
2. Click **Actions** tab
3. Click **Medallion Pipeline** workflow
4. Watch jobs execute sequentially

**Workflow Triggers**:
```yaml
on:
  push:
    branches: [main, develop]      # On code push
  pull_request:
    branches: [main]                # On pull requests
  schedule:
    - cron: '0 2 * * *'             # Daily at 2 AM UTC
  workflow_dispatch:                # Manual trigger
```

#### Step 4: Enable Production Writes (Optional)

To write to actual LakeBase instead of dry-run:

Edit `.github/workflows/medallion-pipeline.yml`:
```yaml
lakebase-sync:
  environment:
    DRY_RUN: 'false'  # Change from 'true' to 'false'
```

### Option 3: Databricks Asset Bundles (real pipeline, deployed from GitHub Actions)

This is the actual production path: `bronze`/`silver`/`gold` are dbt models under `models/` (materialized as `streaming_table`/`materialized_view`, which Databricks backs with a managed Lakeflow Declarative Pipeline automatically), deployed and run via a [Databricks Asset Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/) (`databricks.yml` + `resources/`) — no local Databricks CLI required, everything runs through GitHub Actions:

- `.github/workflows/pr-validate.yml` — secret scan (gitleaks) + `databricks bundle validate` on every PR.
- `.github/workflows/deploy-dev.yml` — deploys and runs the `medallion_pipeline_job` against the `dev` target on push to `develop`.
- `.github/workflows/deploy-prod.yml` — same, against `prod`, gated by a GitHub Environment (`production`) for manual approval, on push to `main`.

The job runs `dbt run`/`dbt test` (bronze → silver → gold, in dependency order), then a final `lakebase_sync` task that writes `gold_customer_metrics` out to the operational Postgres (LakeBase) database. Auth is Service Principal OAuth M2M — no long-lived PAT is stored anywhere in this repo or in GitHub Secrets beyond the SP's client ID/secret. See the plan/setup notes for the one-time manual steps (service principal, Unity Catalog Volume, SQL warehouse, secret scope) required before the first successful deploy.

**Two dbt authoring styles, same outcome**: alongside the SQL models (`models/bronze`, `models/silver`, `models/gold`), `models/python_showcase/` reaches the identical bronze→silver→gold result using dbt **Python models** (PySpark DataFrame code) instead of SQL — a side-by-side comparison of both approaches on the same platform, deployed through the same bundle/job.

---

## Running the Pipeline

### Quick Start

```bash
# 1. Activate virtual environment
source .venv/bin/activate

# 2. Load environment variables
export $(grep -v '^#' .env | xargs)

# 3. Run end-to-end pipeline
python scripts/run_pipeline.py
```

### Detailed Execution

#### Method 1: Individual Layers (for development/debugging)

```bash
cd scripts/

# Layer 1: Bronze Ingestion
echo "=== Running Bronze Layer ===" 
python 00_bronze_ingestion.py
# Expected: 7 raw records ingested

# Layer 2: Silver Transformation
echo "=== Running Silver Layer ===" 
python 01_silver_transformation.py
# Expected: 5 clean records (2 removed, 1 deduplicated)

# Layer 3: Gold Aggregation
echo "=== Running Gold Layer ===" 
python 02_gold_aggregation.py
# Expected: 4 customer summaries with metrics

# Layer 4: LakeBase Sync
echo "=== Running LakeBase Sync ===" 
python 03_lakebase_sync.py
# Expected: Dry-run simulation (or actual write if enabled)
```

#### Method 2: End-to-End Orchestrator

```bash
python scripts/run_pipeline.py
# Executes all 4 layers sequentially with error handling
# Exit code: 0 (success) or 1 (failure)
```

#### Method 3: GitHub Actions (Automated)

```bash
# Push to main branch
git push origin main

# OR manually trigger
# Go to: GitHub → Actions → Medallion Pipeline → Run Workflow
```

### Running Tests

```bash
# Run all tests
pytest tests/ -v --cov=scripts

# Run specific test file
pytest tests/test_bronze.py -v
pytest tests/test_silver.py -v
pytest tests/test_gold.py -v

# Run with coverage report
pytest tests/ --cov=scripts --cov-report=html
# Open htmlcov/index.html in browser
```

---

## Pipeline Components

### Shared Utilities (`scripts/utils.py`)

| Function | Purpose | Usage |
|----------|---------|-------|
| `setup_logger(name, level)` | Configure structured logging | `logger = setup_logger("MyStep")` |
| `log_step(step_num, name, logger)` | Format step headers | `log_step(1, "Bronze Layer", logger)` |
| `log_metrics(metrics, logger)` | Output JSON metrics | `log_metrics({"records": 100}, logger)` |
| `validate_dataframe(df, requirements)` | Schema validation | `validate_dataframe(df, schema)` |
| `benchmark_operation()` | Timing decorator | `@benchmark_operation()` |

### Layer Scripts

#### Bronze: `scripts/00_bronze_ingestion.py`
- **Input**: Mock orders data or Kafka/S3 source
- **Output**: Raw Delta table with ingestion metadata
- **Metrics**: total_records, ingestion_timestamp
- **Execution**: `python 00_bronze_ingestion.py`

#### Silver: `scripts/01_silver_transformation.py`
- **Input**: Bronze raw data
- **Output**: Clean, deduplicated, flattened Delta table
- **Metrics**: quality_rate (%), duplicates_removed, records_removed
- **Execution**: `python 01_silver_transformation.py`

#### Gold: `scripts/02_gold_aggregation.py`
- **Input**: Silver clean data
- **Output**: Customer metrics with VIP identification
- **Metrics**: unique_customers, vip_customers, avg_lifetime_spend
- **Execution**: `python 02_gold_aggregation.py`

#### LakeBase: `scripts/03_lakebase_sync.py`
- **Input**: Gold customer metrics
- **Output**: PostgreSQL operational table
- **Metrics**: records_synced, sync_status, target_table
- **Execution**: `python 03_lakebase_sync.py`

#### Orchestrator: `scripts/run_pipeline.py`
- **Coordinates**: All 4 layers sequentially
- **Error Handling**: Continues on errors, logs all failures
- **Exit Codes**: 0 (success), 1 (failure)
- **Execution**: `python run_pipeline.py`

---

## Monitoring & Troubleshooting

### Logs & Metrics

Each layer produces structured JSON logs:

```bash
# View all logs
tail -f scripts/*.log

# Filter by layer
grep "BRONZE" scripts/*.log
grep "SILVER" scripts/*.log
grep "GOLD" scripts/*.log

# Extract metrics
grep "METRICS:" scripts/*.log | python -m json.tool
```

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **Import errors** | Missing dependencies | `pip install -r scripts/requirements.txt` |
| **No data in output** | Previous layer failed | Check logs of previous layer |
| **Path not found** | Wrong BASE_PATH in config | Edit `scripts/config.py` |
| **LakeBase connection error** | Missing credentials | Populate LAKEBASE_* in .env |
| **Workflow not running** | Actions not enabled | Enable in GitHub Settings |
| **Out of memory** | Large dataset | Reduce mock data size or increase Spark memory |

### Debug Mode

```bash
# Run with verbose logging
LOG_LEVEL=DEBUG python scripts/00_bronze_ingestion.py

# Run single layer with dry-run
DRY_RUN=true python scripts/03_lakebase_sync.py

# Run with profiling
python -m cProfile -s cumtime scripts/run_pipeline.py
```

### GitHub Actions Troubleshooting

1. **View workflow run**: Click Actions tab → Select run → View job logs
2. **Download artifacts**: Click run → Download logs artifact
3. **Check secrets**: Settings → Secrets → Verify all required secrets present
4. **Re-run job**: Click run → Re-run failed jobs

---

## Extending the Pipeline

### Add a New Transformation Layer

1. **Create new script** `scripts/04_custom_layer.py`:

```python
from utils import setup_logger, log_metrics
from config import SILVER_PATH, GOLD_PATH

def transform_layer(spark, logger):
    """Your custom transformation logic."""
    df = spark.read.format("delta").load(SILVER_PATH)
    # Apply transformations...
    df_output = df.withColumn(...)
    df_output.write.format("delta").mode("overwrite").save(GOLD_PATH)
    log_metrics({"records": df_output.count()}, logger)
    return GOLD_PATH

if __name__ == "__main__":
    logger = setup_logger("CustomLayer")
    spark = SparkSession.builder.appName("CustomLayer").getOrCreate()
    transform_layer(spark, logger)
```

2. **Add to orchestrator** `scripts/run_pipeline.py`:

```python
# In main() function
from scripts import _04_custom_layer

try:
    log_step(4, "Custom Layer", logger)
    _04_custom_layer.transform_layer(spark, logger)
except Exception as e:
    logger.error(f"Custom layer failed: {e}")
    failed_steps.append("04_custom_layer")
```

3. **Add to GitHub Actions** `.github/workflows/medallion-pipeline.yml`:

```yaml
custom-layer:
  needs: [gold-aggregation]
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v3
    - uses: actions/setup-python@v4
      with:
        python-version: '3.10'
    - run: |
        pip install pyspark delta-spark
        cd scripts && python 04_custom_layer.py
    - uses: actions/upload-artifact@v3
      with:
        name: custom-layer-logs
        path: scripts/custom-*.log
```

### Connect Real Data Source

Replace mock data in `scripts/00_bronze_ingestion.py`:

```python
# Instead of:
# raw_data = [(ORD101, ...), ...]

# Read from Kafka:
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-broker:9092")
    .option("subscribe", "orders_topic")
    .load()
)

# Or read from S3:
df_raw = spark.read.parquet("s3://your-bucket/orders/")

# Or read from Delta stream:
df_raw = spark.readStream.format("delta").load("s3://bronze-bucket/orders")
```

### Add DBT Models

The bronze/silver/gold dbt models already exist under `models/` (`models/bronze/bronze_orders.sql`, `models/silver/silver_orders.sql`, `models/gold/gold_customer_metrics.sql`), plus a Python-model showcase of the same pipeline under `models/python_showcase/`. To add a new layer or model, follow the same pattern — a `.sql` (or `.py`) file under `models/<layer>/`, a matching `schema.yml` with tests, then:

```bash
dbt deps
dbt run --select <your_new_model>
dbt test --select <your_new_model>
```

See [Option 3: Databricks Asset Bundles](#option-3-databricks-asset-bundles-real-pipeline-deployed-from-github-actions) above for how these models get deployed and run in practice (via `resources/medallion_job.yml`'s `dbt_task`, driven by GitHub Actions — no local dbt/Databricks CLI needed).

---

## FAQ

### Q: Can I run this on Databricks Community Edition?
**A**: Yes! The pipeline works with Community Edition. Note that Databricks Connect is not supported on Community Edition, so run the pipeline within Databricks notebooks or via GitHub Actions instead.

### Q: How do I modify the VIP threshold?
**A**: Edit `scripts/config.py`:
```python
VIP_SPEND_THRESHOLD = 1000.0  # Changed from 500.0
```

### Q: Can I disable the dry-run mode for LakeBase?
**A**: Yes, set `DRY_RUN=false`:
```bash
export DRY_RUN=false
python scripts/03_lakebase_sync.py
```

### Q: How long does the pipeline take to run?
**A**: ~10-15 seconds for demo data:
- Bronze: 2-3s
- Silver: 2-3s
- Gold: 2-3s
- LakeBase: 1-2s

### Q: Can I run layers in parallel?
**A**: Not currently (sequential by design for data dependencies). Future versions could parallelize independent aggregations.

### Q: How do I add a new test?
**A**: Create a test file `tests/test_custom.py`:
```python
import unittest
from scripts import _00_bronze_ingestion

class TestCustom(unittest.TestCase):
    def test_something(self):
        result = _00_bronze_ingestion.generate_raw_orders_data()
        self.assertEqual(len(result), 7)
```

Run with: `pytest tests/test_custom.py -v`

### Q: What's the difference between streaming tables and materialized views?
**A**:
- **Streaming Table**: Ingests data incrementally from continuous sources (Kafka, Cloud Storage)
- **Materialized View**: Aggregates and transforms streaming data; refreshes periodically
- **View**: Logical definition; no data stored

---

## Documentation

- **[PIPELINE_QUICKSTART.md](PIPELINE_QUICKSTART.md)**: 5-minute quick reference
- **[scripts/PIPELINE_README.md](scripts/PIPELINE_README.md)**: Detailed technical guide
- **[DECOMPOSITION_ANALYSIS.md](DECOMPOSITION_ANALYSIS.md)**: Monolithic → modular refactoring breakdown
- **[PROJECT_MANIFEST.md](PROJECT_MANIFEST.md)**: Complete file inventory

---

## Support & Contributing

### Getting Help
1. Check [FAQ](#faq) section
2. Review [scripts/PIPELINE_README.md](scripts/PIPELINE_README.md) for technical details
3. Check GitHub Issues for similar problems
4. Open a new Issue with error logs

### Contributing
1. Create a feature branch
2. Make changes and test locally
3. Update documentation
4. Submit pull request

---

## License

This project is part of the Databricks Labs initiative.

---

## Project Status

| Component | Status |
|-----------|--------|
| Bronze Layer | ✅ Production-Ready |
| Silver Layer | ✅ Production-Ready |
| Gold Layer | ✅ Production-Ready |
| LakeBase Sync | ✅ Production-Ready |
| Orchestrator | ✅ Production-Ready |
| Tests | ✅ 9 tests, full coverage |
| GitHub Actions | ✅ 5 jobs, sequential orchestration |
| Documentation | ✅ Comprehensive |

**Overall Status**: ✅ **PRODUCTION-READY**

---

## Quick Links

- 📖 [Quick Start](PIPELINE_QUICKSTART.md)
- 📚 [Detailed Guide](scripts/PIPELINE_README.md)
- 🏗️ [Architecture](DECOMPOSITION_ANALYSIS.md)
- 📋 [File Inventory](PROJECT_MANIFEST.md)
- 🔗 [GitHub Repo](https://github.com/yourusername/DBT-DATABRICKS-streaming-pipes)

---

**Last Updated**: August 20, 2026  
**Version**: 1.0.0 (Production)  
**Maintainer**: Databricks dbt Team
