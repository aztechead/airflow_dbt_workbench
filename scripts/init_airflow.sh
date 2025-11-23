#!/bin/bash
set -e

echo "Initializing Airflow..."

# Install dbt dependencies
cd /opt/dbt
dbt deps --profiles-dir /opt/dbt

# Initialize Airflow database
airflow db init

# Create admin user
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || true

# Create dbt_duckdb pool for sequential silver/gold execution
airflow pools set dbt_duckdb 1 'Pool for dbt runs with DuckDB attach - prevents lock conflicts'

echo "Airflow initialization complete!"

# Start webserver in background and run scheduler in foreground
airflow webserver &
airflow scheduler
