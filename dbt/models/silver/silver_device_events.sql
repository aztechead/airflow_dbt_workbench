{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}

{% set lookback_minutes = 20 %}
{% set silver_relation = adapter.get_relation(
    database=this.database,
    schema=this.schema,
    identifier=this.identifier
) %}
{% set silver_columns = adapter.get_columns_in_relation(silver_relation) if silver_relation else [] %}
{% set silver_column_names = silver_columns | map(attribute='name') | list %}
{% set silver_has_window = silver_relation is not none and 'event_window_start' in silver_column_names %}

WITH bounds AS (
    {% if is_incremental() and silver_has_window %}
    SELECT
        COALESCE(MAX(event_window_start), TIMESTAMP '1970-01-01') AS max_window,
        COALESCE(MAX(event_window_start), TIMESTAMP '1970-01-01') - INTERVAL '{{ lookback_minutes }}' MINUTE AS lower_bound
    FROM {{ this }}
    {% else %}
    SELECT
        TIMESTAMP '1970-01-01' AS max_window,
        TIMESTAMP '1970-01-01' AS lower_bound
    {% endif %}
),

bronze_events AS (
    SELECT *
    FROM {{ ref('bronze_device_events') }}
    WHERE event_window_start >= (SELECT lower_bound FROM bounds)
),

deduplicated AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY event_id
                ORDER BY loaded_at DESC
            ) AS row_num
        FROM bronze_events
    )
    WHERE row_num = 1
),

new_or_changed AS (
    SELECT n.*
    FROM deduplicated n
    LEFT JOIN (
        {% if is_incremental() and silver_has_window %}
        SELECT event_id, loaded_at
        FROM {{ this }}
        WHERE event_window_start >= (SELECT lower_bound FROM bounds)
        {% else %}
        SELECT NULL::VARCHAR AS event_id, NULL::TIMESTAMP AS loaded_at WHERE FALSE
        {% endif %}
    ) t ON n.event_id = t.event_id
    WHERE t.event_id IS NULL OR n.loaded_at > t.loaded_at
),

unpacked_events AS (
    SELECT
        event_id,
        event_timestamp,
        user_id,
        device_id,
        device_type,
        platform,
        event_type,
        event_date,
        loaded_at,
        event_window_start,
        -- Unpack nested payload fields
        TRY_CAST(payload->>'$.screen_resolution' AS VARCHAR) AS screen_resolution,
        TRY_CAST(payload->>'$.browser' AS VARCHAR) AS browser,
        TRY_CAST(payload->>'$.browser_version' AS VARCHAR) AS browser_version,
        TRY_CAST(payload->'$.location'->>'$.city' AS VARCHAR) AS city,
        TRY_CAST(payload->'$.location'->>'$.country' AS VARCHAR) AS country,
        TRY_CAST(payload->'$.location'->>'$.latitude' AS DOUBLE) AS latitude,
        TRY_CAST(payload->'$.location'->>'$.longitude' AS DOUBLE) AS longitude,
        TRY_CAST(payload->'$.session'->>'$.session_id' AS VARCHAR) AS session_id,
        TRY_CAST(payload->'$.session'->>'$.is_new_session' AS BOOLEAN) AS is_new_session,
        TRY_CAST(payload->'$.performance'->>'$.page_load_time_ms' AS INTEGER) AS page_load_time_ms,
        TRY_CAST(payload->'$.performance'->>'$.network_type' AS VARCHAR) AS network_type
    FROM new_or_changed
),

validated_events AS (
    SELECT *
    FROM unpacked_events
    WHERE
        event_id IS NOT NULL
        AND event_timestamp IS NOT NULL
        AND user_id IS NOT NULL
        AND device_id IS NOT NULL
        AND event_type IS NOT NULL
        AND event_window_start IS NOT NULL
        -- Data quality: remove future events
        AND event_timestamp <= CURRENT_TIMESTAMP
        -- Data quality: remove events older than 30 days
        AND event_timestamp >= CURRENT_TIMESTAMP - INTERVAL 30 DAY
)

SELECT * FROM validated_events
