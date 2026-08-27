{{
  config(
    materialized = 'streaming_table'
  )
}}

-- Raw ingestion via Auto Loader (Lakeflow streaming-first pattern).
-- Demo/lab source: a Unity Catalog Volume landing path seeded with
-- seeds/raw_orders.csv (see seeds/README or the deployment docs for how to
-- populate the volume). In production, point this at the real landing path
-- for your cloud storage or message-bus ingestion.
SELECT
    order_id,
    user_id,
    CAST(amount AS DOUBLE) AS amount,
    status,
    CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
    named_struct('city', shipping_city, 'state', shipping_state) AS shipping_address,
    current_timestamp() AS _ingested_at,
    _metadata.file_path AS _ingestion_source
FROM STREAM read_files(
    -- target.database/target.schema (not a separate var) so this always
    -- matches whatever catalog/schema the active connection is actually
    -- using -- dev vs prod can never drift out of sync with a hardcoded var.
    '/Volumes/{{ target.database }}/{{ target.schema }}/landing/',
    format => 'csv',
    header => true,
    schema => 'order_id STRING, user_id STRING, amount STRING, status STRING, order_timestamp STRING, shipping_city STRING, shipping_state STRING'
)
