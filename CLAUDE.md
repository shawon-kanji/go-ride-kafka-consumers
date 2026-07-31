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
  - `trip-dispatch-worker`: Kafka consumer that matches riders with drivers — real dispatch logic implemented (nearest-driver search via S2/Haversine, job offer creation, radius/backoff retry sweep); publishes `driver.job_offer.created.v1` after each dispatch attempt for `websocket-gateway` to fan out. Driver reject and `ride.unassigned.v1` publishing are not yet implemented.
  - `driver-request-handler`: HTTP API for driver-initiated actions, the driver-side mirror of `cab-request-handler`. `POST /job-offers/{job_offer_id}/accept` implements the first-wins acceptance lock (row-locks the offer + parent trip request, updates `trip_requests`/`ongoing_trips`/`driver_job_offers`/`trip_history` atomically) and publishes `ride.assigned.v1`.
  - `websocket-gateway`: WebSocket gateway pushing job offers to connected drivers and ride assignments to connected riders in realtime (`GET /ws/driver`, `GET /ws/rider`), with Redis-backed presence routing for multi-instance operation and DB-backed reconnect replay for driver offers. Driver reject handling and losing-driver "offer withdrawn" notifications are not yet implemented (see `docs/cab-request-flow.md` Phase 7).
- Every service (and the root module) follows the same internal shape: `cmd/<entrypoint>/main.go` → `internal/bootstrap` (wires config/DB/Kafka/HTTP) → `internal/config`, `internal/kafka` (and/or `internal/api`), `internal/db`/`internal/domain`. Kafka event contracts (JSON-serialized structs) live in the shared [`go-ride-utils/events`](https://github.com/shawon-kanji/go-ride-utils/blob/main/events) package, not a per-service `pkg/events` — see "Shared utilities" below.

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

## Shared utilities

The sibling repo `go-ride-utils` (`github.com/shawon-kanji/go-ride-utils`) holds code shared across services in this repo (and consumed by `go-ride-backend` too):

- [`kafkatopics`](https://github.com/shawon-kanji/go-ride-utils/blob/main/kafkatopics/kafkatopics.go) — the canonical topic-name constants; every service's `internal/config/config.go` uses these as its `KAFKA_*_TOPIC` env var fallback default. There's no schema registry, so event-contract changes must stay backward compatible or be coordinated across producer/consumer services explicitly.
- [`events`](https://github.com/shawon-kanji/go-ride-utils/blob/main/events) — Kafka event contract structs (plain structs, JSON-marshaled onto messages), one file per event type (e.g. `ride_assigned.go`, `job_offer.go`). Replaced each service's old local `pkg/events` copy.
- [`httpheaders`](https://github.com/shawon-kanji/go-ride-utils/blob/main/httpheaders/httpheaders.go) — `Idempotency-Key`/`X-Correlation-ID` header name constants, used in `cab-request-handler`.
- [`awssecrets`](https://github.com/shawon-kanji/go-ride-utils/blob/main/awssecrets/awssecrets.go) — fetches a JSON-valued AWS Secrets Manager secret at startup; see Deployment below.

Consumed via a local `replace` directive in [`go.work`](go.work) pending a tagged release — see that file's comment before assuming `go get @v0.1.0` alone is enough to pick up recent additions like `awssecrets`.

## Deployment

Each service in `services/*` gets its own container image; the root `driver-location-worker` binary is legacy (a no-op consumer stub today) and isn't deployed. Cluster/cloud provisioning (Terraform for VPC/EKS/RDS/MSK/ElastiCache/ECR/IAM, plus local kind tooling) lives in the sibling repo **`go-ride-infra`**, not here — start there for the full picture: [`docs/architecture.md`](https://github.com/shawon-kanji/go-ride-infra/blob/main/docs/architecture.md), [`docs/runbook-cluster.md`](https://github.com/shawon-kanji/go-ride-infra/blob/main/docs/runbook-cluster.md), [`docs/runbook-local.md`](https://github.com/shawon-kanji/go-ride-infra/blob/main/docs/runbook-local.md).

Per-service deployment files (Dockerfile + Helm chart live next to the service, not centrally):

| Service | Dockerfile | Helm chart | Secrets fetched in staging/production |
|---|---|---|---|
| location-producers | `services/location-producers/Dockerfile` | `services/location-producers/deploy/helm/` | none |
| location-consumers | `services/location-consumers/Dockerfile` | `services/location-consumers/deploy/helm/` | DB credentials |
| cab-request-handler | `services/cab-request-handler/Dockerfile` | `services/cab-request-handler/deploy/helm/` | DB credentials |
| driver-request-handler | `services/driver-request-handler/Dockerfile` | `services/driver-request-handler/deploy/helm/` | DB credentials + JWT secret |
| trip-dispatch-worker | `services/trip-dispatch-worker/Dockerfile` | `services/trip-dispatch-worker/deploy/helm/` | DB credentials (chart pins `replicaCount: 1` — see comment in its `values.yaml`, sweep loop isn't safe at >1 yet) |
| websocket-gateway | `services/websocket-gateway/Dockerfile` | `services/websocket-gateway/deploy/helm/` | DB credentials + JWT secret |
| driver-location-worker (root) | `cmd/driver-location-worker/Dockerfile` | none (legacy, not deployed) | none |

Each chart has `values.yaml` (defaults) plus `values-local.yaml` / `values-staging.yaml` / `values-production.yaml` overrides.

Secrets mechanics: in staging/production, `internal/config/config.go`'s `Load(ctx, ...)` checks for `DB_CREDENTIALS_SECRET_NAME` / `JWT_SECRET_NAME` env vars — if set, it fetches the named AWS Secrets Manager entry via `go-ride-utils/awssecrets` (authenticated through IRSA, no explicit credentials needed) instead of reading `DB_USER`/`DB_PASSWORD`/`JWT_SECRET` directly from the environment. Locally (and in every existing test), those env vars are unset, so `config.Load()` behaves exactly as before this change — see `go-ride-infra`'s `docs/architecture.md` "Secrets" section for why (no k8s `Secret` object or ESO in staging/production; local kind still uses a plain one).

Deploys themselves happen via this repo's own CI/CD (not built yet) — build image → push to the ECR repo `go-ride-infra`'s Terraform creates → `helm upgrade --install` using the service's own chart against the `staging`/`production` namespace. For manual local end-to-end testing use `go-ride-infra/local/deploy-local.sh`.
