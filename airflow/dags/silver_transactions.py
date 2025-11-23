from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator

# Define datasets
bronze_transaction_dataset = Dataset("duckdb://main_bronze/bronze_transaction_events")
silver_transaction_dataset = Dataset("duckdb://main_silver/silver_transactions")

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
    'silver_transactions',
    default_args=default_args,
    description='Transform bronze transactions to silver layer',
    schedule=[bronze_transaction_dataset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=['silver', 'dbt', 'transactions'],
) as dag:

    transform_silver = BashOperator(
        task_id='transform_transactions',
        pool='dbt_duckdb',  # Shared pool to prevent concurrent dbt runs
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} --select silver_transactions',
        outlets=[silver_transaction_dataset],
    )

    test_silver = BashOperator(
        task_id='test_transactions',
        pool='dbt_duckdb',  # Shared pool to prevent concurrent dbt runs
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --select silver_transactions',
    )

    transform_silver >> test_silver
