from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator

# Define datasets - only depends on silver device events
silver_device_dataset = Dataset("duckdb://main_silver/silver_device_events")
gold_platform_dataset = Dataset("duckdb://main_gold/gold_platform_events")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

DBT_PROJECT_DIR = '/opt/dbt'
DBT_PROFILES_DIR = '/opt/dbt'

with DAG(
    'gold_platform_events',
    default_args=default_args,
    description='Aggregate device events by platform (iOS, Android, Windows, etc.)',
    schedule=[silver_device_dataset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=['gold', 'dbt', 'analytics', 'platform'],
) as dag:

    compute_gold = BashOperator(
        task_id='compute_platform_events',
        pool='dbt_duckdb',  # Shared pool to prevent concurrent dbt runs
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} --select gold_platform_events',
        outlets=[gold_platform_dataset],
    )

    test_gold = BashOperator(
        task_id='test_platform_events',
        pool='dbt_duckdb',  # Shared pool to prevent concurrent dbt runs
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --select gold_platform_events',
    )

    compute_gold >> test_gold
