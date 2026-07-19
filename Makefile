COMPOSE ?= docker compose
SERVICE ?= kafka
TOPIC ?= driver.location.updated.v1
LOCATION_TOPIC ?= driver.location.updated.v1
RIDE_REQUESTED_TOPIC ?= ride.requested.v1
RIDE_ASSIGNED_TOPIC ?= ride.assigned.v1
RIDE_UNASSIGNED_TOPIC ?= ride.unassigned.v1

.PHONY: up down restart logs ps topic-create topic-list topic-delete topic-create-all migrate-up migrate-down migrate-version \
	build-location build-location-producers build-location-consumers build-cab build-cab-request-handler build-dispatch test-location test-cab test-cab-request-handler test-dispatch \
	run-location-producers run-location-consumers run-cab-request-handler run-dispatch-api run-dispatch-consumer

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
	$(MAKE) build-location-producers
	$(MAKE) build-location-consumers

build-cab:
	$(MAKE) build-cab-request-handler

build-location-producers:
	cd services/location-producers && go build ./...

build-location-consumers:
	cd services/location-consumers && go build ./...

build-cab-request-handler:
	cd services/cab-request-handler && go build ./...

build-dispatch:
	cd services/trip-dispatch-worker && go build ./...

test-location:
	cd services/location-producers && go test ./...
	cd services/location-consumers && go test ./...

test-cab:
	$(MAKE) test-cab-request-handler

test-cab-request-handler:
	cd services/cab-request-handler && go test ./...

test-dispatch:
	cd services/trip-dispatch-worker && go test ./...

run-location-producers:
	cd services/location-producers && go run ./cmd/api

run-location-consumers:
	cd services/location-consumers && go run ./cmd/consumer

run-cab-request-handler:
	cd services/cab-request-handler && go run ./cmd/api

run-dispatch-api:
	cd services/trip-dispatch-worker && go run ./cmd/api

run-dispatch-consumer:
	cd services/trip-dispatch-worker && go run ./cmd/consumer
