.PHONY: up down nifi-up nifi-down airflow-up airflow-down minio-up minio-down

up:
	docker compose --env-file .env -f "containers/clickhouse/docker-compose.yaml" up --build -d
	docker compose --env-file .env -f "containers/nifi/docker-compose.yaml" up --build -d
	docker compose --env-file .env -f "containers/airflow/docker-compose.yaml" up --build -d
	docker compose --env-file .env -f "containers/minio/docker-compose.yaml" up --build -d

down:
	docker compose --env-file .env -f "containers/clickhouse/docker-compose.yaml" down
	docker compose --env-file .env -f "containers/nifi/docker-compose.yaml" down
	docker compose --env-file .env -f "containers/airflow/docker-compose.yaml" down
	docker compose --env-file .env -f "containers/minio/docker-compose.yaml" down

nifi-up:
	docker compose --env-file .env -f "containers/nifi/docker-compose.yaml" up --build

nifi-down:
	docker compose --env-file .env -f "containers/nifi/docker-compose.yaml" down

airflow-up:
	docker compose --env-file .env -f "containers/airflow/docker-compose.yaml" up --build -d

aiflow-down:
	docker compose --env-file .env -f "containers/airflow/docker-compose.yaml" down

minio-up:
	docker compose --env-file .env -f "containers/minio/docker-compose.yaml" up --build -d

minio-down:
	docker compose --env-file .env -f "containers/minio/docker-compose.yaml" down