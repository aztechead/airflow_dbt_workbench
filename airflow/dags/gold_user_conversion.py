from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator

# Define datasets - depends on BOTH silver user activity and transactions
silver_activity_dataset = Dataset("duckdb://main_silver/silver_user_activity")
silver_transaction_dataset = Dataset("duckdb://main_silver/silver_transactions")
gold_conversion_dataset = Dataset("duckdb://main_gold/gold_user_conversion")

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
    'gold_user_conversion',
    default_args=default_args,
    description='Join user activity with transactions to analyze conversion metrics',
    schedule=[silver_activity_dataset, silver_transaction_dataset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=['gold', 'dbt', 'analytics', 'conversion', 'join'],
) as dag:

    compute_gold = BashOperator(
        task_id='compute_user_conversion',
        pool='dbt_duckdb',  # Shared pool to prevent concurrent dbt runs
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} --select gold_user_conversion',
        outlets=[gold_conversion_dataset],
    )

    test_gold = BashOperator(
        task_id='test_user_conversion',
        pool='dbt_duckdb',  # Shared pool to prevent concurrent dbt runs
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --select gold_user_conversion',
    )

    compute_gold >> test_gold
