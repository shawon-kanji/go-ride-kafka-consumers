# go-ride-kafka-consumers

Minimal boilerplate project for Kafka worker services.

## Structure

- `cmd/driver-location-worker`: first worker entrypoint
- `internal/config`: env config loader
- `internal/bootstrap`: app wiring
- `internal/worker`: worker contract/runner
- `internal/kafka`: Kafka placeholders
- `pkg/events`: event contract structs

## Shared DB schema

Database models and SQL migrations are owned by sibling package `go-ride-db-schema`.
Use the migration helpers:

```bash
make migrate-up
make migrate-version
```

## Run

```bash
go run ./cmd/driver-location-worker
```
