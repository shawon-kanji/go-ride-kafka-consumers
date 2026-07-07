COMPOSE ?= docker compose
SERVICE ?= kafka
TOPIC ?= driver.location.updated.v1

.PHONY: up down restart logs ps topic-create topic-list topic-delete migrate-up migrate-down migrate-version

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart: down up

logs:
	$(COMPOSE) logs -f $(SERVICE)

ps:
	$(COMPOSE) ps

topic-create:
	$(COMPOSE) exec -T kafka kafka-topics.sh --create --if-not-exists --topic $(TOPIC) --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

topic-list:
	$(COMPOSE) exec -T kafka kafka-topics.sh --list --bootstrap-server localhost:9092

topic-delete:
	$(COMPOSE) exec -T kafka kafka-topics.sh --delete --topic $(TOPIC) --bootstrap-server localhost:9092

migrate-up:
	cd ../go-ride-db-schema && go run ./cmd/migrate up

migrate-down:
	cd ../go-ride-db-schema && go run ./cmd/migrate down

migrate-version:
	cd ../go-ride-db-schema && go run ./cmd/migrate version
