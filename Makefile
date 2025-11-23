.PHONY: help up down restart logs clean status check-dbs logs-follow

help:
	@echo "Available commands:"
	@echo "  make up        - Start all services (fully automated)"
	@echo "  make down      - Stop all services"
	@echo "  make restart   - Restart all services"
	@echo "  make logs      - Show recent logs from all services"
	@echo "  make logs-follow - Follow logs in real-time"
	@echo "  make clean     - Remove all data and reset (use before fresh start)"
	@echo "  make status    - Show status of all services"
	@echo "  make check-dbs - List all DuckDB database files"

up:
	docker-compose up -d
	@echo ""
	@echo "Services started successfully!"
	@echo ""
	@echo "Airflow UI: http://localhost:8080 (username: admin, password: admin)"
	@echo "DuckDB UI:  http://localhost:8081"
	@echo ""
	@echo "INFO: The pipeline runs automatically every 10 seconds"
	@echo "INFO: Use ATTACH command in DuckDB UI to query databases (see README.md)"
	@echo ""
	@echo "Run 'make status' to check service status"

down:
	docker-compose down
	@echo "All services stopped"

restart:
	docker-compose restart
	@echo "All services restarted"

logs:
	docker-compose logs --tail=200

logs-follow:
	docker-compose logs -f

clean:
	@echo "Cleaning all data..."
	docker-compose down -v
	rm -rf data/*.parquet
	rm -rf duckdb/*.duckdb*
	rm -rf duckdb/*.wal
	@echo ""
	@echo "All data cleaned!"
	@echo "Run 'make up' to start fresh with automated initialization"

status:
	@echo "Service Status:"
	@docker-compose ps
	@echo ""
	@echo "Database Files:"
	@ls -lh duckdb/*.duckdb 2>/dev/null || echo "No database files yet"

check-dbs:
	@echo "All DuckDB Files:"
	@ls -lh duckdb/ 2>/dev/null || echo "No files in duckdb directory"
	@echo ""
	@echo "Active databases:"
	@ls -1 duckdb/*[!y].duckdb 2>/dev/null | grep -v readonly | grep -v ui_catalog || echo "None"
	@echo ""
