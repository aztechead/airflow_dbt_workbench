FROM apache/airflow:3.1.3-python3.13

USER root
RUN apt-get update && apt-get install -y git libpq-dev gcc && apt-get clean

USER airflow

# Install DBT and DuckDB
RUN pip install --no-cache-dir \
    dbt-duckdb \
    pyarrow \
    pandas \
    psycopg2
