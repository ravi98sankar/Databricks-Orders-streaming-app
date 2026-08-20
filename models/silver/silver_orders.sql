{{
  config(
    materialized = 'materialized_view'
  )
}}

-- materialized_view (not streaming_table): dedup below is an unwatermarked
-- whole-history window, i.e. state aggregation rather than append-only
-- streaming semantics -- see dbt_project.yml comment for the reasoning.

WITH filtered AS (
    SELECT *
    FROM {{ ref('bronze_orders') }}
    WHERE order_id IS NOT NULL
      AND user_id IS NOT NULL
      AND amount IS NOT NULL
      AND amount > {{ var('min_valid_amount') }}
      AND status != 'CANCELLED'
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY order_timestamp DESC
        ) AS row_num
    FROM filtered
)

SELECT
    order_id,
    user_id,
    amount,
    status,
    order_timestamp,
    shipping_address.city AS shipping_city,
    shipping_address.state AS shipping_state,
    current_timestamp() AS _processed_at,
    'silver' AS _processing_stage
FROM deduped
WHERE row_num = 1
