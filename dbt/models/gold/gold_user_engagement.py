import pandas as pd

def model(dbt, session):
    """
    Aggregate user engagement metrics with Python for complex analytics.

    This model demonstrates Python for:
    - Complex window functions and analytics
    - Advanced aggregations
    - Statistical calculations
    """
    dbt.config(
        materialized="table",
        packages=["pandas"]
    )

    # Load silver layer data
    silver_events = dbt.ref("silver_device_events")

    df = silver_events.df()

    # Ensure timestamp is datetime
    df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
    df['event_date'] = pd.to_datetime(df['event_date'])

    # Calculate user engagement metrics
    user_metrics = df.groupby(['user_id', 'event_date']).agg(
        total_events=('event_id', 'count'),
        unique_sessions=('session_id', 'nunique'),
        unique_devices=('device_id', 'nunique'),
        event_types_count=('event_type', 'nunique'),
        avg_page_load_time_ms=('page_load_time_ms', 'mean'),
        max_page_load_time_ms=('page_load_time_ms', 'max'),
        min_page_load_time_ms=('page_load_time_ms', 'min'),
        first_event_time=('event_timestamp', 'min'),
        last_event_time=('event_timestamp', 'max')
    ).reset_index()

    # Calculate session duration in minutes
    user_metrics['session_duration_minutes'] = (
        (user_metrics['last_event_time'] - user_metrics['first_event_time'])
        .dt.total_seconds() / 60
    )

    # Calculate engagement score (weighted metric)
    user_metrics['engagement_score'] = (
        user_metrics['total_events'] * 1.0 +
        user_metrics['unique_sessions'] * 5.0 +
        user_metrics['event_types_count'] * 3.0 +
        user_metrics['session_duration_minutes'] * 0.5
    )

    # Add percentile ranks for engagement
    user_metrics['engagement_percentile'] = (
        user_metrics['engagement_score'].rank(pct=True) * 100
    )

    # Categorize users by engagement
    def categorize_engagement(percentile):
        if percentile >= 90:
            return 'highly_engaged'
        elif percentile >= 70:
            return 'engaged'
        elif percentile >= 40:
            return 'moderate'
        else:
            return 'low'

    user_metrics['engagement_category'] = user_metrics['engagement_percentile'].apply(
        categorize_engagement
    )

    # Join back to get device and platform info
    device_info = df.groupby('user_id').agg(
        primary_device=('device_type', lambda x: x.mode()[0] if len(x.mode()) > 0 else None),
        primary_platform=('platform', lambda x: x.mode()[0] if len(x.mode()) > 0 else None),
        primary_browser=('browser', lambda x: x.mode()[0] if len(x.mode()) > 0 else None)
    ).reset_index()

    # Final join
    result = user_metrics.merge(device_info, on='user_id', how='left')

    return result
