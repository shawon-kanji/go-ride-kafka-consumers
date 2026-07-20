## Plan: Cab Request And Dispatch Realtime Flow

Recommended approach: implement schema-first in go-ride-db-schema, then add a new HTTP producer service (cab-request-handler) and a real trip-dispatch consumer with WebSocket fanout integration. Keep Kafka as system backbone: request API writes DB state + publishes ride.requested.v1; dispatcher consumes, finds nearest drivers from latest location table, persists driver job offers, and pushes realtime offers with durable replay from DB.

**Plan revision (2026-07-19): fare-estimate/book split.**
Original Phase 4 combined fare computation, `trip_requests` creation, and Kafka publish into one atomic `POST /request-cab` call. This is being split into two steps so a rider can see and confirm a fare before a search actually starts:
1. `POST /fare-estimate` — computes and locks a fare quote (`trip_fares` row only), with no `trip_requests` row and no Kafka event. Returns `fare_id`, fare breakdown, `expires_at`.
2. `POST /request-cab` — now takes `rider_id` + `fare_id` (+ idempotency/correlation keys) instead of raw pickup/dropoff. It loads the locked, unexpired, unused `trip_fares` row, creates `trip_requests` pointing at it, and only then publishes `ride.requested.v1`. If the fare is expired, it rejects (`fare_expired`) rather than silently repricing — the client must call `/fare-estimate` again.

This requires decoupling `trip_fares` from `trip_requests` in the schema (today `trip_fares.request_id` is `NOT NULL UNIQUE` with an FK to `trip_requests`, which assumes the request exists first — the inverse of what's needed).

**Chosen mechanism**: keep the existing FK and `UNIQUE` constraint on `trip_fares.request_id`, just drop `NOT NULL` (Postgres `UNIQUE` allows multiple `NULL`s, so this is safe). `request_id IS NULL` means "unconsumed, bookable" quote; `IS NOT NULL` means "already booked." Booking claims a fare via a conditional `UPDATE trip_fares SET request_id = ? WHERE fare_id = ? AND request_id IS NULL` inside the same transaction as the `trip_requests` insert, checking `RowsAffected == 1` — this is the race-safe "first booker wins" guard for concurrent `/request-cab` calls against the same `fare_id`, with no explicit row locking needed. `trip_fares` also gains `rider_id`, `pickup_lat/lng`, `dropoff_lat/lng`, `pickup_geohash`, `pickup_s2_cell_id`, `search_radius_km` so a fare estimate is fully self-contained and `/request-cab` only needs `rider_id` + `fare_id`.

Current naming note:
- Database schema and Go models now standardize on `trip_id` for the business trip identifier.
- Go models now use `ID` for primary key fields where practical.
- Existing Kafka topic and event names still use the `ride.*` convention (`ride.requested.v1`, `ride.assigned.v1`, `ride.unassigned.v1`) and should be migrated separately if full terminology alignment is desired.

Current completion status:
- Phase 1 completed as design/spec.
- Phase 2 completed in `go-ride-db-schema` with migrations applied.
- Phase 3 completed in `go-ride-db-schema`, tagged as `v0.2.0`, and adopted in `go-ride-kafka-consumers` root and `services/location-consumers` modules.
- Phase 4 completed in `go-ride-kafka-consumers` with the new `services/cab-request-handler` HTTP producer service (original single-call design, since superseded by Phase 4a).
- Phase 4a completed: `go-ride-db-schema` migration `000013` shipped and tagged `v0.3.0`; `cab-request-handler` now exposes `POST /fare-estimate` and a redesigned `POST /request-cab`. Verified end-to-end against a live local Postgres/Kafka: estimate → book → Kafka publish → `/current-trip` reflects it → idempotent replay → double-booking rejected (`fare_already_used`) → unknown fare rejected (`fare_not_found`) → expired fare rejected (`fare_expired`, verified against a real TTL wait, not just clock math). Dependency bump to `go-ride-db-schema v0.3.0` and the handler code are implemented locally but not yet committed/pushed in `go-ride-kafka-consumers`.

**Steps**
1. Phase 1 - Domain contracts and status model (blocks all later phases) [completed]
   - Finalize canonical naming: use `fare` everywhere (not `fair`) in new artifacts.
   - Define request lifecycle states: `search_started`, `searching`, `offered`, `driver_accepted`, `driver_rejected`, `timed_out`, `assigned`, `cancelled`.
   - Define idempotency/correlation strategy: `request_id`, `trip_id`, `rider_id`, `correlation_id`, and producer idempotency key.
   - Define realtime delivery guarantees: at-least-once delivery over WebSocket; dedupe on `job_offer_id` and `request_id`.


**Phase 1 Implementation Package (Completed As Design Spec)**
- Naming convention: use `fare` in all new table names, columns, events, topics, env vars, docs, and APIs. Avoid introducing `fair` anywhere new.
- Canonical request state machine:
  - `search_started`: API accepted and persisted trip request shell.
  - `searching`: dispatcher started candidate lookup.
  - `offered`: at least one job offer persisted/sent.
  - `driver_accepted`: one driver accepted (pre-assignment lock).
  - `driver_rejected`: candidate rejected (per-offer, not terminal request state).
  - `timed_out`: no winning acceptance before TTL.
  - `assigned`: winning driver locked and assignment emitted.
  - `cancelled`: rider/system cancelled before assignment completion.
- State transition guardrails:
  - Terminal states: `assigned`, `cancelled`, `timed_out`.
  - No transitions out of terminal states except explicit manual recovery workflow.
  - Acceptance race policy: first valid acceptance wins with atomic DB condition check.
- Entity identifiers and correlation:
   - `request_id`: primary id for a search attempt (UUID).
   - `trip_id`: business trip lifecycle id that survives retries/re-offers.
  - `rider_id`: authenticated rider principal.
  - `fare_id`: nullable in first write, set once fare lock snapshot created.
  - `correlation_id`: propagated API -> Kafka -> dispatch -> websocket -> DB audit rows.
  - `idempotency_key`: client-provided key scoped by rider and endpoint.
- Idempotency rules:
  - Same (`rider_id`, `idempotency_key`) within TTL returns existing `request_id` and current status.
  - Request creation and event publish must be outbox-backed or transactionally coordinated to avoid dual-write gaps.
  - Dispatcher consumer must be idempotent by (`request_id`, `event_id`) dedupe check.
- Realtime delivery contract:
  - Delivery semantics: at-least-once over WebSocket.
  - Client dedupe keys: `job_offer_id`, `request_id`, `offer_version`.
  - Ack states: `delivered`, `seen`, `accepted`, `rejected`, `expired`.
  - Reconnect replay source of truth: DB `driver_job_offers` pending non-expired rows.
- Event contract baseline (Phase 1 decision):
   - `ride.requested.v1` required fields: `request_id`, `trip_id`, `rider_id`, pickup/dropoff coordinates, `requested_at`, `fare_id` (nullable), `correlation_id`, `event_id`.
  - Partition key recommendation: `request_id` for balanced dispatch and per-request ordering.
- Security and trust boundaries:
  - Rider app may suggest metadata but server owns fare calculation, search status, and assignment truth.
  - Driver accept/reject commands must validate auth principal == driver_id bound to websocket session.
- SLA targets for downstream implementation:
  - P95 API response for request creation: <= 300ms (excluding dispatch completion).
  - P95 request-to-first-offer emission: <= 3s under nominal load.
  - Offer TTL initial default: 15s (configurable).

2. Phase 2 - Schema repo migration set (must be shipped first; depends on 1) [implemented]
   - Add migration for `trip_requests` table (rider request + search status + nullable `fare_id` at creation time).
   - Add migration for `ongoing_trips` table (active ride assignment and progress snapshots).
   - Add migration for `driver_job_offers` table (the durable joblist drivers can re-open after app refresh).
   - Add migration for `trip_history` table (immutable history/audit trail of state transitions and terminal events).
   - Add migration for `trip_fares` table (runtime fare lock snapshot for each request/ride).
   - Add migration for `fare_surcharges` table (dynamic surcharge components applied per request).
   - Add migration for `fare_configs` table (admin-configured base fare policies by city/vehicle/time window).
   - Add indexes/constraints: status index, rider_id index, driver_id index, request_id unique keys, `(status, created_at)` for worker sweeps, and geo query helpers (`driver_locations` geospatial strategy).
   - Forward rename migration added: `000012_rename_ride_id_to_trip_id` to align schema terminology without rewriting already-applied migration history.

3. Phase 3 - Schema models and release flow (depends on 2) [completed]
   - Add/extend schema models in go-ride-db-schema for each new table with GORM tags and constraints matching migrations.
   - Validate migration up/down and model compatibility in schema repo.
   - Publish schema package version/tag and update consuming repos to that version (or local replace during development, then pin tag).
   - Current model convention: use `ID` for primary key struct fields while preserving explicit `gorm:"column:..."` mappings.
   - Release status: schema package published as `v0.2.0`.

4. Phase 4 - Cab request API service (depends on 3) [completed as originally scoped; being revised per fare-estimate/book split above]
   - Create new service module `services/cab-request-handler` (API + Kafka producer + DB access), following existing location-producers structure.
   - Endpoint: POST request for cab; validate payload; compute preliminary fare lock (create `trip_fares` record) and create `trip_requests` row with `status=search_started`, `fare_id` nullable until locked record exists.
   - Publish `ride.requested.v1` with request metadata and location fields to Kafka.
   - Return immediate response (`search started`) with `request_id`, `trip_id`, `status`, optional ETA window.
   - Add idempotency behavior: repeated client request key returns existing open request safely.

4a. Phase 4a - Fare estimate/lock split (revision; depends on 4, supersedes Phase 4 booking behavior) [implemented]
   - Schema migration `000013` in `go-ride-db-schema`: dropped `NOT NULL` on `trip_fares.request_id` (kept FK + `UNIQUE` — Postgres allows multiple `NULL`s); added `rider_id`, `pickup_lat/lng`, `dropoff_lat/lng`, `pickup_geohash`, `pickup_s2_cell_id`, `search_radius_km` columns to `trip_fares` so an estimate is self-contained before any `trip_requests` row exists (existing rows backfilled by joining the then-linked `trip_requests` row). `trip_requests.fare_id` stays as-is (already nullable, indexed, no FK) and is the sole link direction: request → fare. `models/trip_fare.go`'s `RequestID` field is now `*uuid.UUID` (breaking Go API change); added `TripFare.IsConsumed()`/`IsExpired(now)` helpers. Verified up/down/up round-trip against a live Postgres.
   - Schema module tagged and released as `v0.3.0` (commit `229106c`), following the same commit → tag → release flow used for v0.2.0/v0.2.1.
   - `POST /fare-estimate` in `cab-request-handler`: accepts rider_id + pickup/dropoff + optional search_radius_km; computes fare via `buildFareEstimate` (renamed from `buildTripFare`, same haversine/rate math); persists a standalone `trip_fares` row (`request_id=NULL`) with `locked_at`/`expires_at`; does NOT create a `trip_requests` row and does NOT publish to Kafka. Returns `fare_id`, fare breakdown, `currency_code`, `expires_at` (HTTP 201). No idempotency dedupe on this endpoint — every call mints a fresh quote; unused quotes simply expire.
   - Redesigned `POST /request-cab`: request body is now `rider_id` + `fare_id` + `idempotency_key`/`correlation_id` (no more raw lat/lng in the booking call). Loads the `trip_fares` row by `fare_id`; validates it exists and belongs to `rider_id`, is not expired, and has `request_id IS NULL`; creates `trip_requests` with pickup/dropoff/geohash/search-radius copied from the locked fare row, then atomically claims the fare in the same transaction (`UPDATE trip_fares SET request_id=? WHERE fare_id=? AND request_id IS NULL`, checking `RowsAffected==1`); only then publishes `ride.requested.v1`. Existing idempotency-key dedupe (rider_id + idempotency_key → existing request, looked up via the request's `fare_id`) is preserved.
   - Error responses (JSON body `{error, message}` via `writeJSONError`, sentinel errors `errFareNotFound`/`errFareExpired`/`errFareAlreadyUsed`): `fare_not_found` (404 — also returned when `fare_id` belongs to a different rider, to avoid leaking existence), `fare_expired` (409, client must re-estimate — no silent repricing), `fare_already_used` (409 — covers both "already booked earlier" and "lost a concurrent claim race").
   - `fare_configs`/`fare_surcharges` tables remain unused for now (static env-driven fare config continues); wiring them in is a separate future enhancement, not part of this split.
   - Verified live end-to-end (see "Completed through Phase 4a" below) against the local Postgres/Kafka containers; not yet committed/pushed in `go-ride-kafka-consumers`.

5. Phase 5 - Real dispatcher worker (depends on 3; parallel with 4 once contracts fixed) [implemented]
   - Replaced the noop consumer in `trip-dispatch-worker` with a real `DispatchConsumer` (`internal/kafka/consumer.go`) consuming `ride.requested.v1`, following the same `FetchMessage`/`invalidMessageError`/`CommitMessages` pattern already used by `location-consumers`.
   - Schema migration `000014` in `go-ride-db-schema`: added `dispatch_attempt_count`, `dispatch_radius_km`, `next_dispatch_at` (all nullable/defaulted) to `trip_requests` to persist radius-expansion/backoff retry state across attempts and service restarts; added `DriverJobOffer` status/delivery-status Go consts (`DriverJobOfferStatusPending/Accepted/Rejected/Expired/Withdrawn`, `DriverJobOfferDeliveryStatusPending/Sent/Delivered/Seen/Failed`) matching the existing DB check constraints. Verified up/down/up round-trip. Tagged and released as `v0.3.1`.
   - Core dispatch logic lives in `internal/dispatch/service.go` (`Service.AttemptDispatch(ctx, requestID)`), called both from the Kafka consumer (initial attempt) and a new periodic sweep loop (`internal/worker/sweep_runner.go`, retry attempts) — single shared code path, no duplication. Each attempt runs in one DB transaction: row-locks the `trip_requests` row (`SELECT ... FOR UPDATE`), guards idempotency (no-op unless status is `search_started`/`searching`), runs a Haversine-based nearest-drivers query, and branches:
     - **Drivers found**: bulk-inserts `driver_job_offers` (rank 0-based by ascending distance, `status=pending`, `delivery_status=pending`, `expires_at = now + JOB_OFFER_TTL_SECONDS` (default 15s)), transitions `trip_requests.status` to `offered`, writes a `dispatch_offers_created` trip_history row.
     - **Zero drivers, attempts remain**: grows radius by a fixed `+5km` step (capped at `30km`), schedules `next_dispatch_at` via exponential backoff (base 2s, capped 30s), stays in `searching`.
     - **Zero drivers, attempts exhausted** (5 total: 1 initial + 4 retries): transitions to `timed_out`, sets `timed_out_at`, writes a `dispatch_timed_out` trip_history row.
   - Nearest-driver query: raw SQL CTE computing Haversine distance from `driver_locations` (plain lat/lng, no PostGIS/GiST in this schema), filtered by `recorded_at` freshness (`DRIVER_LOCATION_FRESH_WINDOW_SECONDS`, default 300s — the availability proxy, since `drivers` has no online/presence column) and excluding drivers with an active `ongoing_trips` row (reusing `models.ActiveOngoingTripStatuses()`), ordered by distance, limited to `NEAREST_DRIVERS_LIMIT` (default 10).
   - The sweep loop (`internal/worker/sweep_runner.go`) runs concurrently with the Kafka consumer in `bootstrap/app.go`, polling `trip_requests WHERE status='searching' AND next_dispatch_at <= now() AND dispatch_attempt_count < max` every `DISPATCH_SWEEP_INTERVAL_SECONDS` (default 3s). Its failures are logged and swallowed per-item/per-tick (no offset/redelivery mechanism to lean on, unlike the Kafka consumer) so a transient DB blip never cancels live event consumption.
   - Verified live end-to-end: happy path (4 seeded drivers, offers created in correct distance order, `status=offered`); zero-driver path (radius grew 20→25→30 exactly, backoff 2s→4s→8s→16s exactly, timed out after 5 attempts); idempotency guard (re-publishing an already-`offered` request's event is a logged no-op, no duplicate offers). Test data cleaned up afterward.
   - Emitting `ride.assigned.v1`/`ride.unassigned.v1` and driver accept/reject handling are Phase 7, not part of this phase.

6. Phase 6 - Realtime gateway and WebSocket architecture (depends on 3; can run parallel with 4/5)
   - Introduce dedicated driver realtime gateway service for WebSocket connection management (recommended over embedding in dispatcher).
   - WebSocket connection model:
     - One long-lived WebSocket per logged-in driver device session.
     - Optional second connection for rider tracking channel if needed later.
     - Presence registry keyed by `driver_id` + `device_id`.
   - Dispatch integration:
     - Dispatcher publishes internal notification events (or directly calls gateway queue/topic) with job offers.
     - Gateway pushes offers to online drivers and records delivery/ack status.
   - Offline/reconnect behavior:
     - On reconnect, gateway queries `driver_job_offers` non-expired rows and replays pending offers.

7. Phase 7 - Driver response handling (depends on 6 and 5)
   - Add driver accept/reject path (WebSocket message commands with HTTP fallback endpoint).
   - Apply optimistic concurrency/first-wins lock for acceptance to avoid double assignment.
   - Update `trip_requests`, `ongoing_trips`, `driver_job_offers`, and `trip_history` atomically.
   - Publish `ride.assigned.v1` / `ride.unassigned.v1` for downstream services.

8. Phase 8 - Operational controls, retries, and observability (depends on 4-7)
   - Dead-letter topic for malformed or repeatedly failing request events.
   - Retry policy with bounded backoff for transient DB/network faults.
   - Metrics: request-to-offer latency, offer acceptance rate, timeout ratio, per-partition lag, websocket connected drivers, delivery failures.
   - Tracing/correlation: propagate `correlation_id` across API->Kafka->dispatcher->gateway->DB writes.

9. Phase 9 - Rollout strategy (depends on 8)
   - Deploy schema migration first in safe window.
   - Deploy cab-request-handler with feature flag to shadow-write before full traffic.
   - Deploy dispatcher in canary mode (subset topics/groups) and verify consistency.
   - Enable realtime gateway fanout incrementally by city/tenant/percentage.

**Relevant files**
- /Users/shawonkanji/Documents/projects/go-ride-kafka-consumers/services/trip-dispatch-worker/internal/kafka/consumer.go — replace noop behavior with real ride request consumption and dispatch orchestration.
- /Users/shawonkanji/Documents/projects/go-ride-kafka-consumers/services/trip-dispatch-worker/internal/bootstrap/app.go — wire DB, consumer, producers, and gateway client dependencies.
- /Users/shawonkanji/Documents/projects/go-ride-kafka-consumers/services/trip-dispatch-worker/internal/config/config.go — add DB, websocket/gateway, and timeout/retry configuration knobs.
- /Users/shawonkanji/Documents/projects/go-ride-kafka-consumers/services/trip-dispatch-worker/pkg/events/ride_request.go — extend event contract with request metadata/fare lock fields.
- /Users/shawonkanji/Documents/projects/go-ride-kafka-consumers/services/cab-request-handler/internal/api/server.go — HTTP request handler; adding `POST /fare-estimate` and redesigning `POST /request-cab` to consume a locked `fare_id` instead of raw pickup/dropoff.
- /Users/shawonkanji/Documents/projects/go-ride-kafka-consumers/services/cab-request-handler/internal/api/spatial.go — geohash/S2 helpers reused by the new fare-estimate handler.
- /Users/shawonkanji/Documents/projects/go-ride-kafka-consumers/services/cab-request-handler/internal/kafka/producer.go — Kafka producer used by the cab request API.
- /Users/shawonkanji/Documents/projects/go-ride-kafka-consumers/services/cab-request-handler/pkg/events/ride_requested.go — event contract, populated from the locked fare row at booking time.
- /Users/shawonkanji/Documents/projects/go-ride-kafka-consumers/services/cab-request-handler/internal/bootstrap/app.go — wires config, DB, Kafka producer, and HTTP server.
- /Users/shawonkanji/Documents/projects/go-ride-kafka-consumers/Makefile — add build/test/run targets for new cab-request-handler and realtime gateway services.
- /Users/shawonkanji/Documents/projects/go-ride-kafka-consumers/go.work — include new modules under services.
- go-ride-db-schema/migrations/* — add ordered migration files for trip requests, offers, fares, surcharges, ongoing, history; new `000013` migration to decouple `trip_fares` from `trip_requests` and add pickup/dropoff/geohash/search-radius columns to `trip_fares`.
- go-ride-db-schema/models/* — add corresponding GORM models and relations; update `models/trip_fare.go` for the decoupled/self-contained fare estimate.

**Implemented schema/model alignment**
- `trip_requests.request_id` remains the primary key column; the Go model field is now `ID`.
- `trip_requests.trip_id` is the business trip identifier column.
- `ongoing_trips.id` is now the primary key column; `ongoing_trips.trip_id` is the business trip identifier.
- `driver_job_offers.trip_id` and `trip_history.trip_id` now align with schema naming.
- Historical migration files were not rewritten; terminology alignment was introduced through forward migration `000012`.

**Completed through Phase 3**
- Phase 1 design decisions are documented and frozen for implementation.
- Phase 2 migrations `000005` through `000012` exist in `go-ride-db-schema` and were applied to the `go_ride` database.
- Phase 3 models were added in `go-ride-db-schema`, validated with `go test ./...`, and released as tag `v0.2.0`.
- `go-ride-kafka-consumers` now references `github.com/shawon-kanji/go-ride-db-schema v0.2.0` in both the root module and `services/location-consumers`.

**Completed through Phase 4a**
- `go-ride-db-schema`: migration `000013` applied/rolled-back/re-applied cleanly against a live local Postgres; `models/trip_fare.go` updated; released and pushed as tag `v0.3.0`.
- `go-ride-kafka-consumers` dependency on `go-ride-db-schema` bumped to `v0.3.0` in root, `services/location-consumers`, and `services/cab-request-handler`; all workspace modules (`location-producers`, `location-consumers`, `cab-request-handler`, `trip-dispatch-worker`) plus the root module build and vet clean.
- `cab-request-handler` end-to-end manual verification against local Postgres + Kafka containers:
  - `POST /fare-estimate` → `201`, fare row created with `request_id IS NULL`.
  - `POST /request-cab` with that `fare_id` → `202`, fare row claimed (`request_id` set), `ride.requested.v1` published with the full fare breakdown and location fields, `GET /current-trip` reflects the active request.
  - Same `idempotency_key` replayed → same `request_id`/`trip_id` returned (no duplicate booking).
  - Same `fare_id` booked again with a different `idempotency_key` → `409 fare_already_used`.
  - Unknown `fare_id` → `404 fare_not_found`.
  - Fare booked after real TTL expiry (`FARE_LOCK_TTL_MINUTES=1`, waited past it) → `409 fare_expired`.
- Test data created during manual verification was cleaned up from the `go_ride` database afterward.
- Not yet done: committing/pushing the `go-ride-kafka-consumers` changes (dependency bump + handler code + this doc update).

**Verification**
1. Schema verification
   - Run migration `up` on empty DB and existing DB snapshot.
   - Run migration `down` for latest revision rollback safety.
   - Validate new indexes and FK constraints with explain plans for nearest-driver and pending-offers queries.
2. Contract verification
   - Validate JSON schema for ride.requested.v1 and assigned/unassigned outputs.
   - Ensure idempotent request replay returns same request status.
3. Integration verification
   - API request -> `trip_requests` row created -> Kafka event published.
   - Dispatcher consumes event -> inserts 10 offers -> websocket fanout to online drivers.
   - Driver reconnect sees pending offers from DB.
   - Accept flow marks winner and emits assignment event once.
4. Load and failure verification
   - Synthetic load test for request spikes and websocket fanout.
   - Simulate dispatcher restart and confirm no duplicate assignment.
   - Simulate gateway disconnect/reconnect and verify replay semantics.

**Decisions**
- Assume dedicated `cab-request-handler` service under `services/`.
- Assume dedicated realtime gateway service for WebSocket connections.
- Assume schema-first publishing to go-ride-db-schema before consumer/producer code rollout.
- Standardize on `fare` naming in all new tables and events.
- Use distance-first nearest-driver selection now; ETA-aware ranking deferred.

**Further Considerations**
1. Geospatial query strategy recommendation: use PostGIS geography + GiST for 20km radius search if available; fallback to S2/geohash prefilter plus precise Haversine check.
2. Acceptance race policy recommendation: first accepted valid offer wins; all others get immediate offer-expired event.
3. Timeout recommendation: initial 12-20s offer TTL with configurable retries before marking unassigned.
