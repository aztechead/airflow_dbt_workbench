# Airflow + dbt + DuckDB Medallion Pipeline

Multi-layer data pipeline (Bronze → Silver → Gold) using Airflow orchestration, dbt transformations, and DuckDB storage.

## Quick Start

```bash
make up    # Start all services
make down  # Stop all services
make clean # Remove all data and reset
```

**Access:**
- Airflow UI: http://localhost:8080 (username: `admin`, password: `admin`)
- DuckDB UI: http://localhost:8081

## Architecture

```
Event Generator (every 10s)
  ↓ parquet files
Bronze Layer (raw data, incremental load)
  ├── bronze_device_events
  ├── bronze_session_events
  └── bronze_transaction_events
  ↓
Silver Layer (cleaned, deduplicated, unpacked JSON)
  ├── silver_device_events
  ├── silver_user_activity (device + session joined)
  └── silver_transactions
  ↓
Gold Layer (aggregated analytics)
  ├── gold_platform_events
  └── gold_user_conversion
```

## Database Files

Each bronze table has its own database file for parallel execution:

```
duckdb/
  ├── bronze_device.duckdb       # Device events
  ├── bronze_session.duckdb      # Session events
  ├── bronze_transaction.duckdb  # Transaction data
  ├── silver.duckdb              # All silver tables
  ├── gold.duckdb                # All gold tables
  └── ui_catalog.db              # DuckDB UI (separate)
```

## Querying with DuckDB UI

DuckDB uses file-level locking - you cannot query while the pipeline is writing.

### Quick Query (Between Pipeline Runs)

1. Open http://localhost:8081
2. ATTACH databases manually:
   ```sql
   ATTACH 'bronze_device.duckdb' AS bronze_device (READ_ONLY);
   ATTACH 'silver.duckdb' AS silver (READ_ONLY);
   ATTACH 'gold.duckdb' AS gold (READ_ONLY);
   ```
3. Query the data:
   ```sql
   SELECT COUNT(*) FROM bronze_device.main.bronze_device_events;
   SELECT * FROM silver.main.silver_device_events LIMIT 10;
   SELECT * FROM gold.main.gold_platform_events;
   ```
4. DETACH when done:
   ```sql
   DETACH bronze_device;
   DETACH silver;
   DETACH gold;
   ```

### Extended Query Session

Stop Airflow temporarily to avoid lock conflicts:

```bash
docker-compose stop airflow
# Query via UI at http://localhost:8081
docker-compose start airflow  # Restart when done
```

## Makefile Commands

```bash
make help        # Show available commands
make up          # Start all services (fully automated)
make down        # Stop all services
make status      # Show service status
make check-dbs   # List all database files
make logs        # Show recent logs
make logs-follow # Follow logs in real-time
make clean       # Remove all data (use before fresh start)
```

## Troubleshooting

**Lock errors ("Could not set lock on file")**
- Pipeline is writing. Wait 10 seconds between runs, then query.
- Or stop Airflow: `docker-compose stop airflow`

**No data in tables**
- Check parquet files: `ls -lh data/`
- Check DAG status in Airflow UI
- View logs: `make logs-follow`

**DAG failures**
- Common cause: Trying to query while pipeline is running
- Check Airflow UI for error details
