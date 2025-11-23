-- Initialize DuckDB UI server on port 8081
INSTALL ui;
LOAD ui;

-- Start the UI server on port 8081
SET ui_local_port=8081;
CALL start_ui_server();
