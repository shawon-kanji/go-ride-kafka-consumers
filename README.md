# go-ride-kafka-workers

Monorepo for Kafka-driven, independently deployable worker services.

## Services

- `services/location-worker`: driver location ingest and persistence worker
- `services/trip-dispatch-worker`: ride request dispatch worker

Each service has its own `go.mod`, runtime entrypoints, and env configuration.

## Workspace

- Root `go.work` connects both service modules for local development.
- Root `Makefile` provides per-service test/build/run commands.

## Structure

- `services/location-worker/cmd/api`: location API process
- `services/location-worker/cmd/consumer`: location consumer process
- `services/trip-dispatch-worker/cmd/api`: dispatch API/operator process
- `services/trip-dispatch-worker/cmd/consumer`: dispatch consumer process

## Shared DB schema

Database models and SQL migrations are owned by sibling package `go-ride-db-schema`.
Use the migration helpers:

```bash
make migrate-up
make migrate-version
```

## Run service processes

```bash
make run-location-api
make run-location-consumer
make run-dispatch-api
make run-dispatch-consumer
```

## Build and test

```bash
make test-location
make test-dispatch
make build-location
make build-dispatch
```

## Create Kafka topics

```bash
make topic-create-all
```
