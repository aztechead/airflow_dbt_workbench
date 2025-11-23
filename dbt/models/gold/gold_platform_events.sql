{{
    config(
        materialized='table'
    )
}}

-- Gold layer: Event metrics aggregated by platform
-- This provides a summary of event counts and information by platform

WITH device_events AS (
    SELECT
        platform,
        event_type,
        device_type,
        event_date,
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(DISTINCT device_id) AS unique_devices
    FROM {{ ref('silver_device_events') }}
    GROUP BY platform, event_type, device_type, event_date
),

aggregated_by_platform AS (
    SELECT
        platform,
        SUM(event_count) AS total_events,
        SUM(unique_users) AS total_unique_users,
        SUM(unique_devices) AS total_unique_devices,
        COUNT(DISTINCT event_date) AS days_active,
        MIN(event_date) AS first_event_date,
        MAX(event_date) AS last_event_date,
        -- Event type breakdown
        SUM(CASE WHEN event_type = 'page_view' THEN event_count ELSE 0 END) AS page_view_events,
        SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END) AS click_events,
        SUM(CASE WHEN event_type = 'scroll' THEN event_count ELSE 0 END) AS scroll_events,
        SUM(CASE WHEN event_type = 'video_start' THEN event_count ELSE 0 END) AS video_start_events,
        SUM(CASE WHEN event_type = 'video_complete' THEN event_count ELSE 0 END) AS video_complete_events,
        SUM(CASE WHEN event_type = 'form_submit' THEN event_count ELSE 0 END) AS form_submit_events,
        -- Device type breakdown
        SUM(CASE WHEN device_type = 'mobile' THEN event_count ELSE 0 END) AS mobile_events,
        SUM(CASE WHEN device_type = 'tablet' THEN event_count ELSE 0 END) AS tablet_events,
        SUM(CASE WHEN device_type = 'laptop' THEN event_count ELSE 0 END) AS laptop_events,
        SUM(CASE WHEN device_type = 'desktop' THEN event_count ELSE 0 END) AS desktop_events,
        CURRENT_TIMESTAMP AS aggregated_at
    FROM device_events
    GROUP BY platform
),

enriched_metrics AS (
    SELECT
        platform,
        total_events,
        total_unique_users,
        total_unique_devices,
        days_active,
        first_event_date,
        last_event_date,
        -- Event metrics
        page_view_events,
        click_events,
        scroll_events,
        video_start_events,
        video_complete_events,
        form_submit_events,
        -- Device metrics
        mobile_events,
        tablet_events,
        laptop_events,
        desktop_events,
        -- Calculated metrics
        ROUND(CAST(total_events AS DOUBLE) / NULLIF(total_unique_users, 0), 2) AS avg_events_per_user,
        ROUND(CAST(total_events AS DOUBLE) / NULLIF(days_active, 0), 2) AS avg_events_per_day,
        ROUND(CAST(mobile_events AS DOUBLE) / NULLIF(total_events, 0) * 100, 2) AS mobile_event_pct,
        ROUND(CAST(page_view_events AS DOUBLE) / NULLIF(total_events, 0) * 100, 2) AS page_view_pct,
        aggregated_at
    FROM aggregated_by_platform
)

SELECT * FROM enriched_metrics
ORDER BY total_events DESC
