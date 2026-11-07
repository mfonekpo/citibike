.PHONY: up down nifi-up nifi-down airflow-up airflow-down minio-up minio-down init-db\
		superset-up superset-down superset-logs clickhouse-up clickhouse-down
up:
	docker compose --env-file .env -f "containers/clickhouse/docker-compose.yaml" up --build -d
	docker compose --env-file .env -f "containers/nifi/docker-compose.yaml" up --build -d
	docker compose --env-file .env -f "containers/minio/docker-compose.yaml" up --build -d
	docker compose --env-file .env -f "containers/airflow/docker-compose.yaml" up --build -d
	docker compose --env-file .env -f "containers/superset/docker-compose.yaml" up --build -d

down:
	docker compose --env-file .env -f "containers/clickhouse/docker-compose.yaml" down
	docker compose --env-file .env -f "containers/nifi/docker-compose.yaml" down
	docker compose --env-file .env -f "containers/minio/docker-compose.yaml" down
	docker compose --env-file .env -f "containers/airflow/docker-compose.yaml" down
	docker compose --env-file .env -f "containers/superset/docker-compose.yaml" down

nifi-up:
	docker compose --env-file .env -f "containers/nifi/docker-compose.yaml" up -d

nifi-down:
	docker compose --env-file .env -f "containers/nifi/docker-compose.yaml" down

airflow-up:
	docker compose --env-file .env -f "containers/airflow/docker-compose.yaml" up --build -d

airflow-down:
	docker compose --env-file .env -f "containers/airflow/docker-compose.yaml" down

minio-up:
	docker compose --env-file .env -f "containers/minio/docker-compose.yaml" up --build -d

minio-down:
	docker compose --env-file .env -f "containers/minio/docker-compose.yaml" down

init-db:
	docker compose --env-file .env \
		-f containers/airflow/docker-compose.yaml \
		exec airflow-worker \
		/opt/citibike-venv/bin/python -m src.createTable

superset-up:
	docker compose --env-file .env -f "containers/superset/docker-compose.yaml" up --build -d

superset-down:
	docker compose --env-file .env -f "containers/superset/docker-compose.yaml" down

superset-logs:
	docker compose --env-file .env -f "containers/superset/docker-compose.yaml" logs -f

clickhouse-up:
	docker compose --env-file .env -f "containers/clickhouse/docker-compose.yaml" up -d

clickhouse-down:
	docker compose --env-file .env -f "containers/clickhouse/docker-compose.yaml" down