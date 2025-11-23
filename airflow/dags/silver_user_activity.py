from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator

# Define datasets - this depends on BOTH bronze device and session events
bronze_device_dataset = Dataset("duckdb://main_bronze/bronze_device_events")
bronze_session_dataset = Dataset("duckdb://main_bronze/bronze_session_events")
silver_activity_dataset = Dataset("duckdb://main_silver/silver_user_activity")

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
    'silver_user_activity',
    default_args=default_args,
    description='Join bronze device events with session events to create user activity',
    schedule=[bronze_device_dataset, bronze_session_dataset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=['silver', 'dbt', 'user-activity', 'join'],
) as dag:

    transform_silver = BashOperator(
        task_id='transform_user_activity',
        pool='dbt_duckdb',  # Shared pool to prevent concurrent dbt runs
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} --select silver_user_activity',
        outlets=[silver_activity_dataset],
    )

    test_silver = BashOperator(
        task_id='test_user_activity',
        pool='dbt_duckdb',  # Shared pool to prevent concurrent dbt runs
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --select silver_user_activity',
    )

    transform_silver >> test_silver
