from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator

# Define datasets
bronze_device_dataset = Dataset("duckdb://main_bronze/bronze_device_events")
silver_device_dataset = Dataset("duckdb://main_silver/silver_device_events")

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
    'silver_device_events',
    default_args=default_args,
    description='Transform bronze device events to silver layer',
    schedule=[bronze_device_dataset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=['silver', 'dbt', 'device-events'],
) as dag:

    transform_silver = BashOperator(
        task_id='transform_silver_devices',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} --select silver_device_events',
        outlets=[silver_device_dataset],
        pool='dbt_duckdb',  # Shared pool to prevent concurrent dbt runs with attach
    )

    test_silver = BashOperator(
        task_id='test_silver_devices',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --select silver_device_events',
        pool='dbt_duckdb',
    )

    transform_silver >> test_silver
