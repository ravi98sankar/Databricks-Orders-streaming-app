# Databricks dbt project instructions

This repository targets Databricks with Unity Catalog enabled.

## Required architecture

- Use Lakeflow streaming-first patterns for ingestion.
- Bronze models must use `materialized='streaming_table'` with `read_files()` or cloud message bus sources.
- Silver models must use `materialized='streaming_table'` with `stream(ref('...'))` for incremental streaming logic, or `materialized='materialized_view'` for deduplication/state aggregation.
- Gold models must use `materialized='materialized_view'` for business rollups and aggregations.
- Prefer Unity Catalog names and proper catalog/schema/table naming.

## Testing and governance

- Add native dbt tests for uniqueness, not-null, relationships, and accepted values.
- Use `dbt-expectations` for freshness and range validations when appropriate.
- Add Databricks expectations and table/column tags for governance and ownership.
- Document assumptions and source-to-target lineage clearly.

## Default behavior for generated models

When generating model SQL:

1. Start with the raw landing layer in bronze.
2. Validate, clean, and deduplicate in silver.
3. Produce business-level aggregates in gold.
4. Include tests with every model when possible.
5. Keep the logic compatible with Databricks SQL and Lakeflow semantics.
