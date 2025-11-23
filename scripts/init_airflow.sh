#!/bin/bash
set -e

echo "Initializing Airflow..."

# Install dbt dependencies
cd /opt/dbt
dbt deps --profiles-dir /opt/dbt

# Initialize Airflow database
airflow db migrate

# Create dbt_duckdb pool for sequential silver/gold execution
airflow pools set dbt_duckdb 1 'Pool for dbt runs with DuckDB attach - prevents lock conflicts'

echo "Airflow initialization complete!"

# Start API server (replaces webserver in Airflow 3) and DAG processor in background, then run scheduler in foreground
airflow api-server &
airflow dag-processor &
airflow scheduler
