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

6. Phase 6 - Realtime gateway and WebSocket architecture (depends on 3; can run parallel with 4/5) [implemented]
   - New service `services/websocket-gateway`: `gorilla/websocket` connection layer (`internal/ws`), verify-only JWT auth (`internal/auth`, same `Claims{UserID, Email, Role}`/HS256/shared-secret shape as `go-ride-backend/infrastructure/security/jwt.go`), Postgres access via the same `go-ride-db-schema` module (`internal/db`), and a Redis-backed presence fan-out (`internal/presence`) for multi-instance operation.
   - WebSocket connection model: `GET /ws/driver?token=<jwt>&device_id=<id>` upgrades one long-lived connection per driver device session; local `internal/ws.Hub` keys connections by `driver_id` -> `device_id`. Rider tracking channel remains out of scope (optional/future, per original plan).
   - Dispatch integration (Kafka, not a direct call): `trip-dispatch-worker`'s `dispatch.Service.AttemptDispatch` publishes a `JobOfferV1` event (topic `driver.job_offer.created.v1`, one event per dispatch attempt, all offers from that attempt batched together, partition key `request_id`) after its DB transaction commits — mirrors the existing publish-after-commit pattern already used by `cab-request-handler`. The gateway's `internal/kafka.OfferConsumer` consumes it and broadcasts the *whole batch* once over Redis pub/sub (`internal/presence.Bus`, channel `driver-offers`) rather than pushing to its local hub directly, since the consuming instance has no guarantee it holds any of the target drivers' connections — every gateway instance subscribes to the same channel, receives the full batch, and independently loops over its offers checking its own local hub (`internal/offers.Deliverer.HandleBroadcast`), delivering only the entries whose driver connection it actually holds. Broadcasting the batch once (rather than once per driver offer) is strictly less work for the same delivery coverage, since a driver's live connection lives on exactly one instance at a time. This is what allows the gateway to run as multiple instances/pods (e.g. on Kubernetes/EKS) with no sticky-session requirement.
   - Delivery/ack tracking: the gateway writes directly to `driver_job_offers.delivery_status`/`delivery_attempts`/`responded_at` (`internal/offers.Store`) — `pending` (set by dispatcher) -> `sent` (gateway, on successful push or replay) -> `delivered`/`seen` (gateway, on matching client ack frame). The `status` column (accepted/rejected/expired/withdrawn) remains exclusively dispatcher-owned; the gateway never writes it. Ack frames also accept `accepted`/`rejected` at the parsing level (so Phase 7 needs no wire-format change) but Phase 6 only records `responded_at` bookkeeping for them — no assignment side effects.
   - Offline/reconnect behavior: on every successful upgrade (first connect and reconnect handled identically), `internal/offers.Replayer` queries pending, non-expired `driver_job_offers` joined against `trip_requests` (for pickup/dropoff, since those aren't denormalized onto the offer row) and replays them — this DB query, not any in-memory queue, is the durable source of truth.
   - `offer_version` (a dedupe key named in the Phase 1 frozen decisions) has no backing schema column yet; the wire message hardcodes `offer_version: 1` as a placeholder until real re-offer/reassignment versioning is needed (deferred, likely Phase 7+).
   - The job offer push also carries `estimated_earning`/`currency_code` so drivers see a payout figure before responding. No commission/driver-share model exists in the schema, so this is computed at read time from a configurable `DRIVER_COMMISSION_RATE` (default `0.20`, same env var name in both `trip-dispatch-worker` and `websocket-gateway` — they must stay in sync manually since there's no shared config source) applied to `trip_fares.total_fare`: `estimated_earning = total_fare * (1 - commission_rate)`. `trip-dispatch-worker` computes it once per dispatch attempt (denormalized onto the `JobOfferV1` Kafka event, alongside pickup/dropoff, to keep the gateway's live-push path join-free); `websocket-gateway`'s reconnect-replay path recomputes it independently via a `LEFT JOIN trip_fares` (through `trip_requests.fare_id`) since replay reads directly from Postgres rather than the Kafka event. This is a straight percentage of the rider's total fare, not a real payout ledger — introducing an actual commission/payout model is a separate future enhancement.
   - Driver accept/reject business logic (first-wins lock, `trip_requests`/`ongoing_trips` updates, `ride.assigned.v1`/`ride.unassigned.v1` publish) is explicitly Phase 7, not part of this phase.

7. Phase 7 - Driver response handling (depends on 6 and 5) [accept path implemented; reject deferred]
   - New service `services/driver-request-handler`: driver's HTTP counterpart to `cab-request-handler`, not a change to `trip-dispatch-worker` (which stays a pure Kafka matching engine) or `websocket-gateway` (which stays the WS transport). Same per-service shape as every other module: `cmd/api` -> `internal/bootstrap` -> `internal/api` (HTTP), `internal/offers` (transactional accept logic), `internal/kafka` (`RideAssignedProducer`), `internal/auth` (JWT verify, `DriverRole`), `internal/db`, `pkg/events` (own `RideAssignedV1` copy).
   - `POST /job-offers/{job_offer_id}/accept` (Bearer driver JWT): row-locks `driver_job_offers` then the parent `trip_requests` (`FOR UPDATE`, same pattern as `trip-dispatch-worker/internal/dispatch/service.go`), verifying the offer still belongs to the caller and is `pending`/unexpired, and that `trip_requests.status` is still `offered` — first-wins: a second driver hitting accept after the winner gets `409 offer_not_winnable`. On success, in one transaction: this offer -> `accepted`, sibling `pending` offers for the same request -> `expired`, `trip_requests.status` -> `assigned` directly (no separate `driver_accepted` transient state — collapsed since this flow has no intermediate confirmation step), a new `ongoing_trips` row created (first code path in the repo to ever write this table), and a `trip_history` audit row. After commit, best-effort-snapshots the driver's latest `driver_locations` row and publishes `ride.assigned.v1` (topic `RIDE_ASSIGNED_TOPIC`, keyed by `request_id`).
   - `websocket-gateway` gained a rider-facing WS leg to receive that event: `GET /ws/rider?token=<jwt>&device_id=<id>` (requires `RiderRole`), a parallel `internal/ws.RiderHub` (rider_id -> device_id, mirrors the driver `Hub`), a second Kafka consumer (`internal/kafka.AssignmentConsumer` on `ride.assigned.v1`), and `internal/assignments` (`Notifier`/`Deliverer`) doing the same broadcast-over-Redis-then-local-filter as the job-offer path, on a separate channel (`REDIS_ASSIGNMENT_CHANNEL`, default `ride-assignments`) so the two fan-outs don't share a topic. Delivery is fire-and-forget (`ride_assigned` WS message, no ack expected) — riders missing it fall back to `cab-request-handler`'s existing `GET /current-trip` polling, which already surfaces the `OngoingTrip.DriverID` once assigned with zero changes needed there.
   - Deferred, not built: a driver reject endpoint/flow, and notifying *losing* drivers over their existing WS connection that their offer was withdrawn (their `driver_job_offers.status` does flip to `expired` in the DB so they're excluded from future dispatch/replay, just not pushed proactively). At accept time the `ride.assigned.v1` payload still carries only a one-time `driver_locations` snapshot — continuous live location during the trip is handled separately, see below.

7c. Continuous driver location streaming to the rider (depends on 7) [implemented]
   - `websocket-gateway` becomes a second, independent consumer of the already-existing `driver.location.updated.v1` topic (own `pkg/events/driver_location.go` copy, own `internal/kafka.LocationConsumer`) — no changes to `location-producers`/`location-consumers`, which keep publishing/persisting exactly as before.
   - Problem: `driver.location.updated.v1` is a fleet-wide, continuous, unauthenticated firehose (every driver, whatever interval their app uses), unlike the low-volume `driver.job_offer.created.v1`/`ride.assigned.v1` topics the existing broadcast-to-every-instance-then-filter-locally pattern was built for. Naively copying that pattern would mean every gateway instance receives a Redis pub/sub message for every idle driver's every ping.
   - Fix: filter *before* broadcasting. `internal/presence.Store` (new, sibling to `presence.Bus`, own `*redis.Client`) holds a `driver_id -> ActiveTrip{rider_id, trip_id, ongoing_trip_id, request_id}` KV mapping, `SET...EX` with `ACTIVE_TRIP_TTL_SECONDS` (default 7200s/2h). `internal/assignments.Notifier.HandleRideAssignedEvent` writes this mapping (best-effort, logged-not-fatal) right after a trip is assigned — it already has both IDs in hand. `internal/kafka.LocationConsumer` -> `internal/tracking.Notifier.HandleDriverLocationUpdatedEvent` does exactly one `Store.GetActiveTrip` lookup per incoming ping; on a miss (the large majority — idle/available drivers) it returns immediately, no broadcast, no log (would spam at this volume). Only on a hit does it marshal a `tracking.LocationBroadcast` (enriched with the resolved `rider_id`, absent from the raw Kafka event) and `presence.Bus.Publish` it on a dedicated channel (`REDIS_LOCATION_CHANNEL`, default `driver-location-updates`) — same broadcast-and-local-filter fan-out as the other two pipelines from there (`internal/tracking.Deliverer.HandleBroadcast` -> `ws.RiderHub.ConnectionsForRider` -> fire-and-forget `driver_location` WS message via `ws.NewDriverLocationMessage`, no ack).
   - Known limitation at the time, since closed by Phase 7d below: the TTL was the *only* stop-forwarding mechanism, since no trip-start event existed yet to clear the mapping early. `presence.Store.ClearActiveTrip` was written but intentionally unused. A location ping processed before its trip's `ride.assigned.v1` has been handled (no ordering guarantee across the two Kafka topics) is still silently dropped and self-heals on the next ping — accepted, not engineered around.

7d. Driver "start trip" API (depends on 7c) [implemented]
   - New route in `driver-request-handler`: `POST /ongoing-trips/{ongoing_trip_id}/start` (Bearer driver JWT), symmetric to the accept endpoint. Single collapsed `assigned -> in_progress` transition (`internal/offers/service.go`'s `StartTrip`) — no separate "driver_arriving"/"arrived" step and no geofence validation against pickup coordinates, same trust-the-driver's-tap model as accept. Row-locks `ongoing_trips` by `id` (`FOR UPDATE`), 404/403/409 on not-found/wrong-driver/wrong-status, updates `status`/`started_at`, writes a `trip_history` row (`event_type="trip_started"`), commits, then publishes a new event `ride.started.v1` (own producer method on the existing `RideAssignedProducer`, now a two-topic producer).
   - `websocket-gateway` consumes it via a new `internal/kafka.RideStartedConsumer` -> new `internal/tripstart` package (`Notifier`/`Deliverer`, own Redis channel `REDIS_TRIP_STARTED_CHANNEL`/`trip-started`, same broadcast-and-local-filter shape as `tracking`/`assignments`). `tripstart.Notifier.HandleRideStartedEvent` does two things: calls `presence.Store.ClearActiveTrip` (best-effort, logged-not-fatal — the exact hook Phase 7c left unused) so the location-streaming filter stops matching that driver, then broadcasts a `trip_started` WS push (fire-and-forget, no ack) so the rider gets an explicit signal rather than `driver_location` pings just silently stopping.
   - Not built: trip completion/drop-off (closed by Phase 7e below); any change to `trip_requests` (`assigned` already covers this — the further `in_progress` state lives entirely on `ongoing_trips`, per the Phase 1 frozen state machine).

7e. Driver "end trip" + "payment collected" APIs (depends on 7d) [implemented]
   - Schema migration `000016` in `go-ride-db-schema`: adds a new `ongoing_trips.status` value `awaiting_payment` (widens the `chk_ongoing_trips_status` CHECK constraint) sitting between `in_progress` and `completed`, plus `ended_at`, `final_fare numeric(12,2)`, `payment_status varchar(20)` (new `chk_ongoing_trips_payment_status` CHECK, values `pending`/`collected`), `payment_collected_at`. `ActiveOngoingTripStatuses()` now includes `awaiting_payment` — its only consumer, `trip-dispatch-worker`'s nearest-driver query, must keep excluding a driver who is still standing with the rider waiting on cash from new dispatch offers. Tagged `v0.3.3`; dependency bumped (separate commit) in every consuming `go.mod` (root, `location-consumers`, `cab-request-handler`, `driver-request-handler`, `trip-dispatch-worker`, `websocket-gateway`).
   - Two new routes in `driver-request-handler`, both symmetric to the start-trip endpoint: `POST /ongoing-trips/{ongoing_trip_id}/end` (`internal/offers/service.go`'s `EndTrip`, `in_progress -> awaiting_payment`) and `POST /ongoing-trips/{ongoing_trip_id}/collect-payment` (`CollectPayment`, `awaiting_payment -> completed`). Same row-lock/validate/update/history/commit/publish-after-commit shape as `StartTrip`. `EndTrip` sets `final_fare` to the already-locked `trip_fares.total_fare` copied as-is — no live recalculation from distance/duration (the platform doesn't track that), same trust-the-driver's-tap model as accept/start-trip. `CollectPayment` is a manual/cash-only confirmation — no payment method field, no gateway integration. Each publishes its own event (`ride.ended.v1` / `ride.completed.v1`, own producer methods on the now-four-topic `RideAssignedProducer`) rather than collapsing into one event with a discriminator, matching the existing one-event-per-driver-action precedent.
   - `websocket-gateway` gained two more consumer/package pairs mirroring `tripstart`: `internal/kafka.RideEndedConsumer` -> `internal/tripend` (`Notifier`/`Deliverer`, `REDIS_TRIP_ENDED_CHANNEL`/`trip-ended`) pushes `trip_ended` (carries `final_fare`/`currency_code`) to the rider; `internal/kafka.RideCompletedConsumer` -> `internal/tripcomplete` (`REDIS_TRIP_COMPLETED_CHANNEL`/`trip-completed`) pushes `trip_completed`. Neither notifier calls `presence.Store.ClearActiveTrip` — that already fired at trip-start (Phase 7d), nothing left to clear. `Run` now fans in 7 goroutines instead of 5.
   - `cab-request-handler`'s `/current-trip` polling fallback updated to match: `awaiting_payment` added to the local `activeOngoingTripStatuses` filter (otherwise a rider polling instead of relying on the WS push would lose visibility into their trip at the exact moment they need to see the amount due) and its own `ongoingTripPayload` gained `EndedAt`/`FinalFare`/`PaymentStatus`/`PaymentCollectedAt`. Deliberately not carrying `currency_code` in this fallback response — the existing `trip_fares` lookup here is already `nil` by the time a trip reaches `awaiting_payment`, and the primary `trip_ended` WS push already carries currency.
   - Not built: any payment gateway or payment method field (cash-only, driver-confirmed); fare recalculation from actual trip distance/duration; any change to `trip_requests` (same rationale as Phase 7d — all post-assignment granularity lives on `ongoing_trips`).

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

**Completed through Phase 6**
- `services/websocket-gateway` added as a new `go.work` module: `internal/ws` (hub/connection/server/protocol), `internal/auth` (JWT verify-only), `internal/presence` (Redis pub/sub bus), `internal/offers` (store/replay/notifier/deliverer), `internal/kafka` (`OfferConsumer`), `internal/bootstrap`, `internal/config`, `internal/db`, `pkg/events` (own `JobOfferV1` copy).
- `trip-dispatch-worker` changes: `internal/kafka/producer.go` (`TripDispatcherProducer.PublishJobOffer`), `internal/config/config.go` (`OfferCreatedTopic`/`KAFKA_OFFER_CREATED_TOPIC`), `pkg/events/job_offer.go` (`JobOfferV1`/`JobOfferEntry`), `internal/dispatch/service.go` (`createOffers` now returns the created offers; `AttemptDispatch` publishes after the transaction commits via `buildJobOfferEvent`), `internal/bootstrap/app.go` (wires the producer into `dispatch.NewService`).
- Repo-level: `go.work` includes the new module; `Makefile` gained `build-`/`test-`/`run-websocket-gateway` targets and `DRIVER_JOB_OFFER_CREATED_TOPIC` in `topic-create-all`; `docker-compose.yml` gained a `redis` service.
- All modules (root, `location-producers`, `location-consumers`, `cab-request-handler`, `trip-dispatch-worker`, `websocket-gateway`) build and vet clean.
- Not yet done: end-to-end manual verification against live Postgres/Kafka/Redis (connect, dispatch, push, ack, replay, multi-instance routing); committing/pushing these changes.

**Completed through Phase 7 (accept path only)**
- `services/driver-request-handler` added as a new `go.work` module (see Phase 7 write-up above for the full shape/flow); `Makefile` gained `build-`/`test-`/`run-driver-request-handler` targets.
- `websocket-gateway` changes: `internal/ws/rider_hub.go` + `rider_connection.go` (new `RiderHub`/`RiderConnection`, mirroring the driver `Hub`/`Connection`), `internal/ws/protocol.go` (`RideAssignedMessage`), `internal/ws/server.go` (`GET /ws/rider`, requires `auth.RiderRole`), `internal/assignments` (new package: `Notifier`/`Deliverer`, mirrors `internal/offers`'s broadcast-and-filter design on a separate Redis channel), `internal/kafka/assignment_consumer.go` (`AssignmentConsumer` on `ride.assigned.v1`), `internal/config/config.go` (`RideAssignedTopic`/`KAFKA_ASSIGNED_TOPIC`, `RedisAssignmentChannel`/`REDIS_ASSIGNMENT_CHANNEL`), `internal/bootstrap/app.go` (wires the second Redis bus/consumer/hub, runs 3 goroutines in `Run` now instead of 2), `pkg/events/ride_assigned.go` (own `RideAssignedV1` copy).
- All modules build/vet clean (`make build-driver-request-handler build-websocket-gateway build-cab build-dispatch`); no unit tests added since no service in this repo currently has any (`go test ./...` reports `[no test files]` everywhere, matching existing convention).
- Not yet done at the time: end-to-end manual verification against live Postgres/Kafka/Redis; driver reject endpoint; losing-driver "offer withdrawn" WS push; live driver-location-during-trip updates. End-to-end verification was subsequently run manually and confirmed working (accept -> `ride_assigned` WS push with driver name/location, first-wins 409 on a second accept attempt).

**Completed through Phase 7c**
- `websocket-gateway` changes: `internal/presence/store.go` (new `Store`/`ActiveTrip`, KV counterpart to `Bus`), `internal/tracking` (new package: `broadcast.go`/`notifier.go`/`deliver.go`), `internal/kafka/location_consumer.go` (`LocationConsumer`, no per-message success log unlike the other two consumers — volume reasons), `internal/assignments/notifier.go` (writes the active-trip mapping, best-effort), `internal/ws/protocol.go` (`DriverLocationMessage`), `internal/config/config.go` (`DriverLocationTopic`/`KAFKA_LOCATION_TOPIC`, `RedisLocationChannel`/`REDIS_LOCATION_CHANNEL`, `ActiveTripTTL`/`ACTIVE_TRIP_TTL_SECONDS`), `internal/bootstrap/app.go` (wires the third Redis bus + the KV store + the third consumer, `Run` now fans in 4 goroutines instead of 3), `pkg/events/driver_location.go` (own `DriverLocationUpdatedV1` copy). `.env.example` also backfilled `KAFKA_ASSIGNED_TOPIC`/`REDIS_ASSIGNMENT_CHANNEL`, missing from the prior phase.
- `location-producers`/`location-consumers` untouched — `websocket-gateway` is purely a second consumer of the existing `driver.location.updated.v1` topic.
- Builds/vets/tests clean (`cd services/websocket-gateway && go build ./... && go vet ./... && go test ./...`).
- End-to-end manually verified against live Postgres/Kafka/Redis: after accept, `driver_location` pings arrived on the rider's socket; a ping for an unrelated (non-assigned) driver produced none, confirming the active-trip filter rather than a blanket broadcast.

**Completed through Phase 7d**
- `driver-request-handler` changes: `pkg/events/ride_started.go` (new `RideStartedV1`), `internal/config/config.go` (`StartedTopic`/`KAFKA_STARTED_TOPIC`), `internal/kafka/producer.go` (`RideAssignedProducer` gains a second writer/topic and a `PublishRideStarted` method), `internal/offers/service.go` (`StartTrip`, `ErrTripNotFound`/`ErrTripForbidden`/`ErrTripNotStartable`, `StartTripResult`), `internal/api/server.go` (`POST /ongoing-trips/{ongoing_trip_id}/start`, `ongoingTripPayload` gains `StartedAt`, shared `ongoingTripPayloadFrom` helper now used by both handlers).
- `websocket-gateway` changes: `pkg/events/ride_started.go` (own `RideStartedV1` copy), `internal/tripstart` (new package: `broadcast.go`/`notifier.go`/`deliver.go` — `Notifier.HandleRideStartedEvent` calls `presence.Store.ClearActiveTrip` then broadcasts), `internal/kafka/ride_started_consumer.go` (`RideStartedConsumer`, normal per-message logging — low volume, unlike `LocationConsumer`), `internal/ws/protocol.go` (`TripStartedMessage`), `internal/config/config.go` (`RideStartedTopic`/`KAFKA_STARTED_TOPIC`, `RedisTripStartedChannel`/`REDIS_TRIP_STARTED_CHANNEL`), `internal/bootstrap/app.go` (wires the fourth Redis bus + fourth consumer, reuses the existing `locationStore`/`riderHub`, `Run` now fans in 5 goroutines instead of 4).
- Root `Makefile`/`docker-compose` topic list: `RIDE_STARTED_TOPIC` added to `topic-create-all`. No new build/test/run targets needed (existing per-service targets already cover the whole service).
- Builds/vets/tests clean in both services.
- Not yet done: end-to-end manual verification of the start-trip path (transition, `trip_started` WS push, location stream actually stopping, double-start returning 409) against live Postgres/Kafka/Redis; committing/pushing these changes.

**Completed through Phase 7e**
- `go-ride-db-schema`: migration `000016` applied/rolled-back/re-applied cleanly against a live local Postgres; `models/ongoing_trip.go` updated (`OngoingTripStatusAwaitingPayment`, `OngoingTripPaymentStatus{Pending,Collected}`, `ActiveOngoingTripStatuses()` widened, 4 new nullable struct fields). Released and pushed as tag `v0.3.3`. Dependency bumped to `v0.3.3` in root, `location-consumers`, `cab-request-handler`, `driver-request-handler`, `trip-dispatch-worker`, `websocket-gateway` `go.mod`s (separate commit from the migration).
- `driver-request-handler` changes: `pkg/events/ride_ended.go` (new `RideEndedV1`), `pkg/events/ride_completed.go` (new `RideCompletedV1`), `internal/config/config.go` (`EndedTopic`/`KAFKA_ENDED_TOPIC`, `CompletedTopic`/`KAFKA_COMPLETED_TOPIC`), `internal/kafka/producer.go` (`RideAssignedProducer` gains two more writer/topic pairs and `PublishRideEnded`/`PublishRideCompleted`), `internal/offers/service.go` (`EndTrip`, `CollectPayment`, `ErrTripNotEndable`/`ErrTripNotCollectable`, `EndTripResult`/`CollectPaymentResult`), `internal/api/server.go` (`POST /ongoing-trips/{ongoing_trip_id}/end`, `POST /ongoing-trips/{ongoing_trip_id}/collect-payment`, `ongoingTripPayload` gains `EndedAt`/`CompletedAt`/`FinalFare`/`PaymentStatus`/`PaymentCollectedAt`, new `endTripResponse`/`collectPaymentResponse` wrapper types carrying `currency_code`).
- `websocket-gateway` changes: `pkg/events/ride_ended.go` + `ride_completed.go` (own copies), `internal/tripend` + `internal/tripcomplete` (new packages, each `broadcast.go`/`notifier.go`/`deliver.go`), `internal/kafka/ride_ended_consumer.go` + `ride_completed_consumer.go`, `internal/ws/protocol.go` (`TripEndedMessage`, `TripCompletedMessage`), `internal/config/config.go` (`RideEndedTopic`/`RideCompletedTopic`, `RedisTripEndedChannel`/`RedisTripCompletedChannel`), `internal/bootstrap/app.go` (wires two more Redis buses + two more consumers, `Run` now fans in 7 goroutines instead of 5). `.env.example` backfilled with all 4 new vars.
- `cab-request-handler` changes: `internal/api/server.go` (`activeOngoingTripStatuses` gains `awaiting_payment`, `ongoingTripPayload` gains `EndedAt`/`FinalFare`/`PaymentStatus`/`PaymentCollectedAt`).
- Root `Makefile` topic list: `RIDE_ENDED_TOPIC`/`RIDE_COMPLETED_TOPIC` added to `topic-create-all`. No `docker-compose.yml` change needed (no explicit topic list there — Kafka auto-create is enabled). No new build/test/run targets needed.
- Builds/vets clean in all touched modules (`go-ride-db-schema`, root, `location-consumers`, `cab-request-handler`, `driver-request-handler`, `trip-dispatch-worker`, `websocket-gateway`); no unit tests added, matching existing repo-wide convention.
- Not yet done: end-to-end manual verification against live Postgres/Kafka/Redis (end trip -> final fare shown -> collect payment -> `completed`, double-end/double-collect return 409, rider WS receives both `trip_ended`/`trip_completed` pushes, `/current-trip` reflects `awaiting_payment` then disappears once `completed`); committing/pushing these changes.

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
