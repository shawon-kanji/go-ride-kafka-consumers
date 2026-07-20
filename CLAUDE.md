# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`go-ride-kafka-consumers` is one of several sibling repos that make up the **go-ride** ride-hailing platform (the others being `go-ride-db-schema` and `go-ride-backend`, checked out alongside this repo). This repo is a monorepo of independently deployable, Kafka-driven Go worker/API services. Kafka is the system backbone: HTTP APIs write DB state and publish events; workers consume events, do their job, and publish downstream events.

The full target architecture (cab request → fare lock → dispatch → driver websocket offers) is documented in `docs/cab-request-flow.md` — read it before making non-trivial changes to the request/dispatch flow, and keep it updated (phase status, endpoint contracts, schema decisions) as that flow evolves.

## Repo layout

- **Root module** (`go-ride-kafka-consumers`, package `go.mod` at repo root): a standalone `driver-location-worker` binary (`cmd/driver-location-worker`) that predates the services split below. It is **not** part of `go.work` and must be built/tested independently (see Commands). Treat it as legacy — new location-ingestion work happens in `services/location-producers` / `services/location-consumers`.
- **`services/*`**: independently versioned Go modules, each with its own `go.mod`, wired together for local dev via the root `go.work`:
  - `location-producers`: HTTP ingest API + Kafka producer for driver location updates.
  - `location-consumers`: Kafka consumer that persists driver locations.
  - `cab-request-handler`: HTTP API + Kafka producer for the rider-facing cab request flow (fare estimate, booking, current-trip polling).
  - `trip-dispatch-worker`: Kafka consumer intended to match riders with drivers; currently still a `NoopConsumer` stub (see `internal/kafka/consumer.go`) — the real dispatch logic (nearest-driver search, job offers, assignment) is not yet implemented.
- Every service (and the root module) follows the same internal shape: `cmd/<entrypoint>/main.go` → `internal/bootstrap` (wires config/DB/Kafka/HTTP) → `internal/config`, `internal/kafka` (and/or `internal/api`), `internal/db`/`internal/domain` → `pkg/events` (Kafka event contracts, JSON-serialized).

## Shared DB schema

Database models and SQL migrations live in the sibling repo `go-ride-db-schema`, **not** in this repo. It's consumed as a versioned, tagged Go module dependency (`github.com/shawon-kanji/go-ride-db-schema`) from the root module and from `services/location-consumers` / `services/cab-request-handler`.

When a schema change is needed:
1. Make the change in `go-ride-db-schema` (new numbered migration + matching GORM model update), verify migration up/down against a real Postgres.
2. Commit, tag a new version there (bump minor/major on breaking Go struct changes, patch on additive-only changes), and push.
3. Bump the dependency in each consuming `go.mod` here (root, `location-consumers`, `cab-request-handler`) via `go get github.com/shawon-kanji/go-ride-db-schema@vX.Y.Z && go mod tidy`, run **as a separate commit** from the feature code that uses the new schema.

Run migrations against your local DB from this repo with:
```bash
make migrate-up
make migrate-down
make migrate-version
```
(these `cd ../go-ride-db-schema && go run ./cmd/migrate ...` — the schema repo must be checked out as a sibling directory).

## Commands

Local infra (Kafka via docker-compose; Postgres is expected to run separately, e.g. from `go-ride-db-schema` or your own container):
```bash
make up            # start Kafka (docker compose)
make down
make topic-create-all   # create all known topics (driver.location.updated.v1, ride.requested.v1, ride.assigned.v1, ride.unassigned.v1)
```

Build/test (per-service; there is no single top-level `go build ./...` — see below):
```bash
make build-location          # location-producers + location-consumers
make build-cab                # cab-request-handler
make build-dispatch            # trip-dispatch-worker
make test-location
make test-cab
make test-dispatch
```

Run a service locally (reads env vars — copy the service's `.env.example` and `export` its values first; there is no dotenv autoloading):
```bash
make run-location-producers
make run-location-consumers
make run-cab-request-handler
make run-dispatch-api
make run-dispatch-consumer
```

Because the root module is excluded from `go.work`, build/vet/test it with `GOWORK=off`:
```bash
GOWORK=off go build ./...
GOWORK=off go test ./...
```
For anything under `services/*`, `cd` into that service's directory first (its own `go.mod`), then run `go build ./...` / `go test ./...` / `go vet ./...` normally — or `go test ./... -run TestName` for a single test.

## Conventions worth knowing

- **Naming**: canonical spelling is `fare` (never `fair`) across tables, columns, events, env vars, and code. The business trip identifier is `trip_id` (Go struct field `TripID`); primary-key struct fields are named `ID` even where the DB column is `request_id`/`fare_id`/etc. Kafka topic/event names still use the older `ride.*` convention (`ride.requested.v1`, `ride.assigned.v1`, `ride.unassigned.v1`) — this is a known, deliberate inconsistency, not a bug.
- **Idempotency**: HTTP request-creation endpoints accept an `Idempotency-Key` header (or `idempotency_key` body field) scoped by rider; replaying the same key returns the existing resource rather than creating a duplicate.
- **Correlation**: `correlation_id` is generated if not supplied by the client and is expected to propagate from API → Kafka event → downstream consumers.
- **Event contracts** live under each service's `pkg/events/` as plain structs JSON-marshaled onto Kafka messages — there's no schema registry, so contract changes must stay backward compatible or be coordinated across producer/consumer services explicitly.
