FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow

# Install DBT and DuckDB
RUN pip install --no-cache-dir \
    dbt-duckdb==1.7.2 \
    pyarrow==15.0.0 \
    pandas==2.2.0
