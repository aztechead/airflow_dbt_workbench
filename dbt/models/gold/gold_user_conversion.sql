{{
    config(
        materialized='table'
    )
}}

-- Gold layer: User conversion metrics
-- Joins silver_user_activity and silver_transactions to show user behavior to purchase conversion

WITH user_activity_metrics AS (
    SELECT
        user_id,
        platform,
        COUNT(DISTINCT event_id) AS total_events,
        COUNT(DISTINCT session_id) AS total_sessions,
        SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END) AS bounce_sessions,
        AVG(session_duration_seconds) AS avg_session_duration,
        AVG(pages_viewed) AS avg_pages_viewed,
        AVG(interactions) AS avg_interactions,
        MIN(event_timestamp) AS first_activity_date,
        MAX(event_timestamp) AS last_activity_date,
        -- Referrer breakdown
        SUM(CASE WHEN referrer = 'organic' THEN 1 ELSE 0 END) AS organic_sessions,
        SUM(CASE WHEN referrer = 'social' THEN 1 ELSE 0 END) AS social_sessions,
        SUM(CASE WHEN referrer = 'paid' THEN 1 ELSE 0 END) AS paid_sessions
    FROM {{ ref('silver_user_activity') }}
    GROUP BY user_id, platform
),

transaction_metrics AS (
    SELECT
        user_id,
        platform,
        COUNT(*) AS total_transactions,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_transaction_value,
        SUM(CASE WHEN is_first_purchase THEN 1 ELSE 0 END) AS first_purchase_count,
        MIN(transaction_timestamp) AS first_purchase_date,
        MAX(transaction_timestamp) AS last_purchase_date,
        COUNT(DISTINCT product_category) AS unique_categories_purchased
    FROM {{ ref('silver_transactions') }}
    GROUP BY user_id, platform
),

joined_metrics AS (
    SELECT
        COALESCE(a.user_id, t.user_id) AS user_id,
        COALESCE(a.platform, t.platform) AS platform,
        -- Activity metrics
        COALESCE(a.total_events, 0) AS total_events,
        COALESCE(a.total_sessions, 0) AS total_sessions,
        COALESCE(a.bounce_sessions, 0) AS bounce_sessions,
        a.avg_session_duration,
        a.avg_pages_viewed,
        a.avg_interactions,
        a.first_activity_date,
        a.last_activity_date,
        a.organic_sessions,
        a.social_sessions,
        a.paid_sessions,
        -- Transaction metrics
        COALESCE(t.total_transactions, 0) AS total_transactions,
        COALESCE(t.total_revenue, 0) AS total_revenue,
        t.avg_transaction_value,
        t.first_purchase_count,
        t.first_purchase_date,
        t.last_purchase_date,
        t.unique_categories_purchased,
        -- Derived metrics
        CASE WHEN a.user_id IS NOT NULL AND t.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_converted,
        CASE
            WHEN t.user_id IS NULL THEN 'Non-Purchaser'
            WHEN t.total_transactions = 1 THEN 'One-Time Buyer'
            WHEN t.total_transactions BETWEEN 2 AND 5 THEN 'Repeat Buyer'
            WHEN t.total_transactions > 5 THEN 'Loyal Customer'
        END AS customer_segment
    FROM user_activity_metrics a
    FULL OUTER JOIN transaction_metrics t
        ON a.user_id = t.user_id
        AND a.platform = t.platform
),

conversion_analysis AS (
    SELECT
        *,
        -- Conversion rate metrics
        CASE
            WHEN total_sessions > 0 THEN
                ROUND(CAST(total_transactions AS DOUBLE) / total_sessions * 100, 2)
            ELSE 0
        END AS session_conversion_rate,
        CASE
            WHEN total_sessions > 0 THEN
                ROUND(CAST(bounce_sessions AS DOUBLE) / total_sessions * 100, 2)
            ELSE 0
        END AS bounce_rate,
        -- Customer lifetime value proxy
        ROUND(total_revenue, 2) AS customer_ltv,
        -- Days between first activity and first purchase
        CASE
            WHEN first_activity_date IS NOT NULL AND first_purchase_date IS NOT NULL THEN
                EXTRACT(DAY FROM (first_purchase_date - first_activity_date))
            ELSE NULL
        END AS days_to_first_purchase,
        CURRENT_TIMESTAMP AS aggregated_at
    FROM joined_metrics
)

SELECT * FROM conversion_analysis
ORDER BY total_revenue DESC, total_events DESC
