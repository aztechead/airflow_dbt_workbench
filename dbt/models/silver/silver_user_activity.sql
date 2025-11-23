{{
    config(
        materialized='incremental',
        unique_key='activity_id'
    )
}}

-- This silver model demonstrates joining two bronze sources:
-- bronze_device_events and bronze_session_events

{% set lookback_minutes = 20 %}
{% set silver_relation = adapter.get_relation(
    database=this.database,
    schema=this.schema,
    identifier=this.identifier
) %}
{% set silver_columns = adapter.get_columns_in_relation(silver_relation) if silver_relation else [] %}
{% set silver_column_names = silver_columns | map(attribute='name') | list %}
{% set silver_has_window = silver_relation is not none and 'activity_window_start' in silver_column_names %}

WITH bounds AS (
    {% if is_incremental() and silver_has_window %}
    SELECT
        COALESCE(MAX(activity_window_start), TIMESTAMP '1970-01-01') AS max_window,
        COALESCE(MAX(activity_window_start), TIMESTAMP '1970-01-01') - INTERVAL '{{ lookback_minutes }}' MINUTE AS lower_bound
    FROM {{ this }}
    {% else %}
    SELECT
        TIMESTAMP '1970-01-01' AS max_window,
        TIMESTAMP '1970-01-01' AS lower_bound
    {% endif %}
),

-- Get recent device events from bronze
bronze_devices AS (
    SELECT *
    FROM {{ ref('bronze_device_events') }}
    WHERE event_window_start >= (SELECT lower_bound FROM bounds)
),

-- Get recent sessions from bronze
bronze_sessions AS (
    SELECT *
    FROM {{ ref('bronze_session_events') }}
    WHERE session_window_start >= (SELECT lower_bound FROM bounds)
),

-- Deduplicate device events
deduped_devices AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY event_id
                ORDER BY loaded_at DESC
            ) AS row_num
        FROM bronze_devices
    )
    WHERE row_num = 1
),

-- Deduplicate sessions
deduped_sessions AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY session_id
                ORDER BY loaded_at DESC
            ) AS row_num
        FROM bronze_sessions
    )
    WHERE row_num = 1
),

-- Join device events with session data
joined_activity AS (
    SELECT
        -- Create composite key
        d.event_id || '_' || COALESCE(s.session_id, 'no_session') AS activity_id,
        d.event_id,
        d.event_timestamp,
        d.user_id,
        d.device_id,
        d.device_type,
        d.platform,
        d.event_type,
        d.event_date,
        d.event_window_start AS activity_window_start,
        -- Session enrichment
        s.session_id,
        s.session_duration_seconds,
        s.pages_viewed,
        s.interactions,
        s.is_bounce,
        s.referrer,
        s.landing_page,
        -- Metadata
        d.loaded_at AS device_loaded_at,
        s.loaded_at AS session_loaded_at,
        CURRENT_TIMESTAMP AS processed_at
    FROM deduped_devices d
    LEFT JOIN deduped_sessions s
        ON d.user_id = s.user_id
        AND d.platform = s.platform
        AND ABS(EXTRACT(EPOCH FROM (d.event_timestamp - s.session_timestamp))) <= 300  -- Within 5 minutes
),

-- Apply data quality rules
validated_activity AS (
    SELECT *
    FROM joined_activity
    WHERE
        event_id IS NOT NULL
        AND event_timestamp IS NOT NULL
        AND user_id IS NOT NULL
        AND event_timestamp <= CURRENT_TIMESTAMP
        AND event_timestamp >= CURRENT_TIMESTAMP - INTERVAL 30 DAY
)

SELECT * FROM validated_activity

{% if is_incremental() and silver_has_window %}
    WHERE activity_window_start > (SELECT max_window FROM bounds) - INTERVAL '{{ lookback_minutes }}' MINUTE
{% endif %}
