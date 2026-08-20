{{
  config(
    materialized = 'materialized_view'
  )
}}

WITH with_metrics AS (
    SELECT
        *,
        SUM(amount) OVER (
            PARTITION BY user_id
            ORDER BY order_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS lifetime_cumulative_spend,
        SUM(amount) OVER (
            PARTITION BY user_id, YEAR(order_timestamp)
            ORDER BY order_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS ytd_cumulative_spend
    FROM {{ ref('silver_orders') }}
),

flagged AS (
    SELECT
        *,
        lifetime_cumulative_spend >= {{ var('vip_spend_threshold') }} AS is_vip_customer
    FROM with_metrics
)

SELECT
    user_id,
    MAX(lifetime_cumulative_spend) AS total_lifetime_spend,
    MAX(ytd_cumulative_spend) AS current_ytd_spend,
    COUNT(order_id) AS total_orders_count,
    AVG(amount) AS avg_order_amount,
    MIN(amount) AS min_order_amount,
    MAX(amount) AS max_order_amount,
    MAX(order_timestamp) AS last_active_timestamp,
    MIN(order_timestamp) AS first_active_timestamp,
    MAX(is_vip_customer) AS is_vip,
    current_timestamp() AS _aggregated_at
FROM flagged
GROUP BY user_id
