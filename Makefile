.PHONY: up down clickhouse minio

up:
	docker compose --env-file .env -f "containers/clickhouse/docker-compose.yaml" up --build -d

down:
	docker compose --env-file .env -f "containers/clickhouse/docker-compose.yaml" down