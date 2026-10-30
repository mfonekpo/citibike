.PHONY: up down nifi-up nifi-down

up:
	docker compose --env-file .env -f "containers/clickhouse/docker-compose.yaml" up --build -d
	docker compose --env-file .env -f "containers/nifi/docker-compose.yaml" up --build -d

down:
	docker compose --env-file .env -f "containers/clickhouse/docker-compose.yaml" down
	docker compose --env-file .env -f "containers/nifi/docker-compose.yaml" down

nifi-up:
	docker compose --env-file .env -f "containers/nifi/docker-compose.yaml" up --build

nifi-down:
	docker compose --env-file .env -f "containers/nifi/docker-compose.yaml" down