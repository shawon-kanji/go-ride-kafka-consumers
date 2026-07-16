COMPOSE ?= docker compose
SERVICE ?= kafka
TOPIC ?= driver.location.updated.v1
LOCATION_TOPIC ?= driver.location.updated.v1
RIDE_REQUESTED_TOPIC ?= ride.requested.v1
RIDE_ASSIGNED_TOPIC ?= ride.assigned.v1
RIDE_UNASSIGNED_TOPIC ?= ride.unassigned.v1

.PHONY: up down restart logs ps topic-create topic-list topic-delete topic-create-all migrate-up migrate-down migrate-version \
	build-location build-dispatch test-location test-dispatch run-location-api run-location-consumer run-dispatch-api run-dispatch-consumer

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

topic-create-all:
	$(COMPOSE) exec -T kafka kafka-topics.sh --create --if-not-exists --topic $(LOCATION_TOPIC) --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
	$(COMPOSE) exec -T kafka kafka-topics.sh --create --if-not-exists --topic $(RIDE_REQUESTED_TOPIC) --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
	$(COMPOSE) exec -T kafka kafka-topics.sh --create --if-not-exists --topic $(RIDE_ASSIGNED_TOPIC) --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
	$(COMPOSE) exec -T kafka kafka-topics.sh --create --if-not-exists --topic $(RIDE_UNASSIGNED_TOPIC) --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

migrate-up:
	cd ../go-ride-db-schema && go run ./cmd/migrate up

migrate-down:
	cd ../go-ride-db-schema && go run ./cmd/migrate down

migrate-version:
	cd ../go-ride-db-schema && go run ./cmd/migrate version

build-location:
	cd services/location-worker && go build ./...

build-dispatch:
	cd services/trip-dispatch-worker && go build ./...

test-location:
	cd services/location-worker && go test ./...

test-dispatch:
	cd services/trip-dispatch-worker && go test ./...

run-location-api:
	cd services/location-worker && go run ./cmd/api

run-location-consumer:
	cd services/location-worker && go run ./cmd/consumer

run-dispatch-api:
	cd services/trip-dispatch-worker && go run ./cmd/api

run-dispatch-consumer:
	cd services/trip-dispatch-worker && go run ./cmd/consumer
