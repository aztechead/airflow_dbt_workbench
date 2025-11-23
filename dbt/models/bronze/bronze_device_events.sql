{{
    config(
        materialized='incremental',
        unique_key='event_id',
        partition_by={'field': 'event_window_start', 'data_type': 'timestamp'}
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
    FROM read_parquet('/opt/data/device_events_*.parquet', union_by_name=true, filename=true)
),

parsed_events AS (
    SELECT
        s.event_id,
        TRY_CAST(s.timestamp AS TIMESTAMP) AS event_timestamp,
        s.user_id,
        s.device_id,
        s.device_type,
        s.platform,
        s.event_type,
        -- Store full payload as JSON for flexibility
        s.payload,
        -- Extract key fields for partitioning and filtering
        TRY_CAST(s.timestamp AS DATE) AS event_date,
        CASE
            WHEN TRY_CAST(s.timestamp AS TIMESTAMP) IS NOT NULL THEN DATE_TRUNC('minute', TRY_CAST(s.timestamp AS TIMESTAMP))
                - ((EXTRACT(minute FROM TRY_CAST(s.timestamp AS TIMESTAMP))::INT % 10) * INTERVAL '1 minute')
        END AS event_window_start,
        s.source_filename,
        CURRENT_TIMESTAMP AS loaded_at
    FROM source_files s
    {% if is_incremental() %}
    CROSS JOIN last_processed lp
    -- Only process files newer than the last processed file
    WHERE s.source_filename > COALESCE(lp.last_file, '')
    {% endif %}
)

SELECT * FROM parsed_events
