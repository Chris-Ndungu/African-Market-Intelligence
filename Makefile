ROOT_DIR := $(CURDIR)

.PHONY: up down logs ps

up:
	docker compose --env-file "$(ROOT_DIR)/.env" -f "$(ROOT_DIR)/infrastructure/docker-compose.yaml" up -d

down:
	docker compose --env-file "$(ROOT_DIR)/.env" -f "$(ROOT_DIR)/infrastructure/docker-compose.yaml" down

logs:
	docker compose --env-file "$(ROOT_DIR)/.env" -f "$(ROOT_DIR)/infrastructure/docker-compose.yaml" logs -f

ps:
	docker compose --env-file "$(ROOT_DIR)/.env" -f "$(ROOT_DIR)/infrastructure/docker-compose.yaml" ps