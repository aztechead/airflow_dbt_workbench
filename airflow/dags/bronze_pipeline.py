from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Define datasets for triggering downstream DAGs
bronze_device_dataset = Dataset("duckdb://main_bronze/bronze_device_events")
bronze_session_dataset = Dataset("duckdb://main_bronze/bronze_session_events")
bronze_transaction_dataset = Dataset("duckdb://main_bronze/bronze_transaction_events")

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

def check_new_data():
    """Check if new parquet files exist."""
    data_dir = Path('/opt/data')
    device_files = list(data_dir.glob('device_events_*.parquet'))
    session_files = list(data_dir.glob('session_events_*.parquet'))
    transaction_files = list(data_dir.glob('transaction_events_*.parquet'))

    total_files = len(device_files) + len(session_files) + len(transaction_files)
    if total_files == 0:
        raise ValueError("No parquet files found")

    print(f"Found {len(device_files)} device event files")
    print(f"Found {len(session_files)} session event files")
    print(f"Found {len(transaction_files)} transaction event files")
    print(f"Total: {total_files} parquet files")
    return total_files

with DAG(
    'bronze_layer',
    default_args=default_args,
    description='Load parquet files into Bronze layer',
    schedule='*/10 * * * * *',  # Every 10 seconds
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,  # Only allow one run at a time to prevent backlog
    tags=['bronze', 'dbt', 'ingestion'],
) as dag:

    check_data = PythonOperator(
        task_id='check_new_data',
        python_callable=check_new_data,
    )

    # Load each bronze table separately using dedicated profiles (no attach)
    # This allows true parallel execution without lock conflicts
    load_bronze_devices = BashOperator(
        task_id='load_bronze_devices',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profile bronze_device --profiles-dir {DBT_PROFILES_DIR} --select bronze_device_events',
        outlets=[bronze_device_dataset],
    )

    load_bronze_sessions = BashOperator(
        task_id='load_bronze_sessions',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profile bronze_session --profiles-dir {DBT_PROFILES_DIR} --select bronze_session_events',
        outlets=[bronze_session_dataset],
    )

    load_bronze_transactions = BashOperator(
        task_id='load_bronze_transactions',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profile bronze_transaction --profiles-dir {DBT_PROFILES_DIR} --select bronze_transaction_events',
        outlets=[bronze_transaction_dataset],
    )

    test_bronze = BashOperator(
        task_id='test_bronze',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profile bronze_device --profiles-dir {DBT_PROFILES_DIR} --select tag:bronze',
    )

    check_data >> [load_bronze_devices, load_bronze_sessions, load_bronze_transactions] >> test_bronze
