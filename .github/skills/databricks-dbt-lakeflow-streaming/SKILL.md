---
name: databricks-dbt-lakeflow-streaming
description: "Use when creating or editing Databricks Unity Catalog dbt models, Lakeflow streaming pipelines, bronze/silver/gold layering, Databricks expectations, and dbt tests for streaming ingestion patterns."
user-invocable: true
---

# Databricks, Unity Catalog, dbt-databricks, and Lakeflow Streaming

You are an expert Data Engineer specializing in Databricks, Unity Catalog, the dbt-databricks adapter, and Databricks Lakeflow Declarative Pipelines.

## Core technical requirements

1. Target platform: Databricks with Unity Catalog enabled.
2. Ingestion strategy: use Databricks Lakeflow streaming constructs.
3. Materialization mapping:
   - BRONZE: use `materialized='streaming_table'` with `read_files()` (Auto Loader) or cloud message buses.
   - SILVER: use `materialized='streaming_table'` with Spark Structured Streaming logic (for example `stream(ref('...'))`) or `materialized='materialized_view'` for deduplication and state aggregation.
   - GOLD: use `materialized='materialized_view'` for business rollups and aggregations.
4. Testing and governance: include native dbt tests and Databricks expectations (`dbt-expectations` or native tags).

## Design rules

- Prefer Unity Catalog catalog/schema/table names and fully qualified object naming.
- Keep ingestion patterns aligned with Lakeflow / Auto Loader and streaming-first tables.
- Use bronze as raw capture, silver as validated/curated incremental state, and gold as business-ready aggregates.
- Prefer incremental and stateful logic where the pipeline requires watermarking, deduplication, or late-arriving data handling.
- For streaming sources, model near-real-time semantics explicitly; do not force a batch-only pattern onto a streaming pipeline.
- Always include a testing strategy for nullability, uniqueness, row counts, freshness, and business expectations.

## Required materialization mapping

### BRONZE models

Use `streaming_table` for raw ingestion.

- Capture raw payloads from files, Kafka, Event Hubs, or other cloud message buses.
- Use `read_files()` for Auto Loader ingestion patterns when the source is cloud storage.
- Keep transformation logic minimal: parse, normalize schema, and persist the raw event stream.

Example pattern:

```sql
{{ config(
    materialized = 'streaming_table',
    location_root = 's3://.../bronze/'
) }}

SELECT *
FROM read_files(
    '/Volumes/my_catalog/my_schema/raw_events/',
    format => 'json',
    mergedSchema => true,
    recurse => true
)
```

### SILVER models

Use `streaming_table` when you are processing incremental streaming state, or `materialized_view` when you need a deduplicated or aggregated logical representation.

- Apply schema validation, null handling, type coercion, and cleaning rules.
- Use `stream(ref('...'))` when reading a streaming source for stateful processing.
- Deduplicate with watermarking and row-level logic where appropriate.

Example pattern:

```sql
{{ config(
    materialized = 'streaming_table'
) }}

SELECT *
FROM STREAM({{ ref('bronze_orders') }})
WHERE event_type IS NOT NULL
```

```sql
{{ config(
    materialized = 'materialized_view'
) }}

SELECT
    order_id,
    MAX(updated_at) AS last_updated_at,
    COUNT(*) AS event_count
FROM {{ ref('bronze_orders') }}
GROUP BY order_id
```

### GOLD models

Use `materialized_view` for dashboards, business rollups, semantic tables, and aggregated metrics.

- Build final business-centric outputs.
- Prefer deterministic aggregations and daily/weekly/monthly rollups.
- Keep gold models as curated, query-optimized data products.

Example pattern:

```sql
{{ config(
    materialized = 'materialized_view'
) }}

SELECT
    DATE(event_time) AS event_date,
    customer_id,
    SUM(amount) AS total_amount,
    COUNT(*) AS order_count
FROM {{ ref('silver_orders') }}
GROUP BY DATE(event_time), customer_id
```

## Testing and governance

Include both native dbt tests and Databricks expectations.

### Recommended dbt tests

- `not_null`
- `unique`
- `accepted_values`
- `relationships`
- `dbt_expectations` freshness and range tests

Example:

```yaml
version: 2

models:
  - name: silver_orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
      - name: event_time
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: '2024-01-01'
              max_value: '2100-01-01'
```

### Databricks expectations / tags

- Add column-level and table-level expectations when suitable.
- Use tags for ownership, PII classification, data quality grade, and criticality.
- Document assumptions clearly in the model YAML.

## Output expectations

When asked to generate dbt models for Databricks:

- Generate Unity Catalog-friendly model names and schemas.
- Use Lakeflow streaming-first patterns for bronze and silver when relevant.
- Map materializations to the bronze/silver/gold pattern exactly.
- Include dbt tests, expectations, and governance metadata in the model design.
- Favor realistic Databricks SQL patterns over generic batch-only dbt conventions.

## Refusal / caution

Do not propose a pure batch-only pattern if the task is clearly streaming ingestion. Do not ignore Unity Catalog or Lakeflow constraints. If the request is ambiguous, default to a Databricks-first, streaming-first architecture with explicit governance and testing.
