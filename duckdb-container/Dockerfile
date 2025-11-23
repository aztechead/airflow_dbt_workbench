FROM python:3.13-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget unzip socat && \
    rm -rf /var/lib/apt/lists/*

RUN wget -q -O /tmp/duckdb_cli.zip https://github.com/duckdb/duckdb/releases/download/v1.4.2/duckdb_cli-linux-amd64.zip && \
    unzip /tmp/duckdb_cli.zip -d /usr/local/bin && \
    rm /tmp/duckdb_cli.zip && \
    chmod +x /usr/local/bin/duckdb

WORKDIR /data
VOLUME /data

COPY start-duckdb-ui.sh /usr/local/bin/start-duckdb-ui.sh
RUN chmod +x /usr/local/bin/start-duckdb-ui.sh

EXPOSE 4213

CMD ["/usr/local/bin/start-duckdb-ui.sh"]
