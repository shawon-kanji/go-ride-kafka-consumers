## Plan: Cab Request And Dispatch Realtime Flow

Recommended approach: implement schema-first in go-ride-db-schema, then add a new HTTP producer service (cab-request-handler) and a real trip-dispatch consumer with WebSocket fanout integration. Keep Kafka as system backbone: request API writes DB state + publishes ride.requested.v1; dispatcher consumes, finds nearest drivers from latest location table, persists driver job offers, and pushes realtime offers with durable replay from DB.

Current naming note:
- Database schema and Go models now standardize on `trip_id` for the business trip identifier.
- Go models now use `ID` for primary key fields where practical.
- Existing Kafka topic and event names still use the `ride.*` convention (`ride.requested.v1`, `ride.assigned.v1`, `ride.unassigned.v1`) and should be migrated separately if full terminology alignment is desired.

Current completion status:
- Phase 1 completed as design/spec.
- Phase 2 completed in `go-ride-db-schema` with migrations applied.
- Phase 3 completed in `go-ride-db-schema`, tagged as `v0.2.0`, and adopted in `go-ride-kafka-consumers` root and `services/location-consumers` modules.

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

4. Phase 4 - Cab request API service (depends on 3)
   - Create new service module `services/cab-request-handler` (API + Kafka producer + DB access), following existing location-producers structure.
   - Endpoint: POST request for cab; validate payload; compute preliminary fare lock (create `trip_fares` record) and create `trip_requests` row with `status=search_started`, `fare_id` nullable until locked record exists.
   - Publish `ride.requested.v1` with request metadata and location fields to Kafka.
   - Return immediate response (`search started`) with `request_id`, `trip_id`, `status`, optional ETA window.
   - Add idempotency behavior: repeated client request key returns existing open request safely.

5. Phase 5 - Real dispatcher worker (depends on 3; parallel with 4 once contracts fixed)
   - Replace noop consumer in trip-dispatch-worker with real Kafka consumer for `ride.requested.v1`.
   - On event: load request state, guard idempotency, set `searching`, query nearest 10 drivers within 20 km from latest location table.
   - Persist offers in `driver_job_offers` with expiry (TTL) and per-driver status (`pending`, `accepted`, `rejected`, `expired`).
   - Emit assignment/unassigned events to existing topics after acceptance timeout logic and winner selection.
   - Write transition records into `trip_history` for every state change.

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
- /Users/shawonkanji/Documents/projects/go-ride-kafka-consumers/Makefile — add build/test/run targets for new cab-request-handler and realtime gateway services.
- /Users/shawonkanji/Documents/projects/go-ride-kafka-consumers/go.work — include new modules under services.
- go-ride-db-schema/migrations/* — add ordered migration files for trip requests, offers, fares, surcharges, ongoing, history.
- go-ride-db-schema/models/* — add corresponding GORM models and relations.

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
