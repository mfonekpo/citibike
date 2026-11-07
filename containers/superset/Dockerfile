FROM apache/superset:6.0.0

USER root

# PostgreSQL for Superset metadata;
# ClickHouse for the Citi Bike analytics warehouse.
RUN uv pip install --python /app/.venv/bin/python \
    "psycopg2-binary>=2.9,<3" \
    "clickhouse-connect>=1.8,<2.0"

COPY --chown=superset:superset \
    superset_config.py /app/pythonpath/superset_config.py

ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

USER superset