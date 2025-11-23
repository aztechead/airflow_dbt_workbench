{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        partition_by={'field': 'transaction_window_start', 'data_type': 'timestamp'}
    )
}}

WITH
{% if is_incremental() %}
last_processed AS (
    SELECT COALESCE(MAX(source_filename), '') AS last_file
    FROM {{ this }}
    WHERE source_filename IS NOT NULL
),
{% endif %}

source_files AS (
    SELECT
        *,
        filename AS source_filename
    FROM read_parquet('/opt/data/transaction_events_*.parquet', union_by_name=true, filename=true)
),

parsed_transactions AS (
    SELECT
        s.transaction_id,
        TRY_CAST(s.timestamp AS TIMESTAMP) AS transaction_timestamp,
        s.user_id,
        s.platform,
        s.product_id,
        s.product_category,
        s.quantity,
        s.unit_price,
        s.total_amount,
        s.currency,
        s.payment_method,
        s.shipping_country,
        s.is_first_purchase,
        -- Partitioning fields
        TRY_CAST(s.timestamp AS DATE) AS transaction_date,
        CASE
            WHEN TRY_CAST(s.timestamp AS TIMESTAMP) IS NOT NULL THEN DATE_TRUNC('minute', TRY_CAST(s.timestamp AS TIMESTAMP))
                - ((EXTRACT(minute FROM TRY_CAST(s.timestamp AS TIMESTAMP))::INT % 10) * INTERVAL '1 minute')
        END AS transaction_window_start,
        s.source_filename,
        CURRENT_TIMESTAMP AS loaded_at
    FROM source_files s
    {% if is_incremental() %}
    CROSS JOIN last_processed lp
    -- Only process files newer than the last processed file
    WHERE s.source_filename > COALESCE(lp.last_file, '')
    {% endif %}
)

SELECT * FROM parsed_transactions
