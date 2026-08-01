# Driver-Initiated Cancellation: Engineering Challenges

Notes on the hardest parts of building driver-initiated trip cancellation (mirroring the earlier rider-cancellation feature) and its automatic-redispatch follow-on. Written up interview-style — each section is a problem, the reasoning behind the fix, and what it would sound like as an answer to "tell me about a hard bug you found."

Not pushed to GitHub — local reference only.

---

## 1. Lock ordering across concurrent trip-mutating flows (deadlock avoidance)

**The problem.** By the time driver-cancellation was built, four different code paths could all touch the same `trip_requests` / `ongoing_trips` row pair under row-level locks: `AcceptOffer` (locks `driver_job_offers` → `trip_requests`), rider-cancellation's `cancelAfterAssignment` (locks `trip_requests` → `ongoing_trips`), the dispatch worker's `AttemptDispatch` (locks `trip_requests` only), and now driver-cancellation needed to lock `ongoing_trips` too. Two flows taking row locks on the same two tables in *opposite* orders is the textbook setup for a database deadlock: transaction A holds table 1, waits on table 2; transaction B holds table 2, waits on table 1; both block forever until one gets killed by the database's deadlock detector.

**The constraint.** The driver-cancellation HTTP endpoint only receives `ongoing_trip_id` in the URL (matching the existing `start`/`end`/`collect-payment` endpoints' convention) — but the established safe lock order, set by rider-cancellation, was "`trip_requests` first, `ongoing_trips` second." Locking `ongoing_trips` first (the "obvious" thing to do, since that's the ID we're handed) would have been the exact reverse order of a concurrent rider-cancel racing on the same trip.

**The fix.** Do an *unlocked* read first — `SELECT request_id FROM ongoing_trips WHERE id = ?` — purely to discover the FK, taking no lock. Then acquire locks in the established order: `trip_requests` first, then re-fetch-and-lock `ongoing_trips` by its actual ID, doing the real ownership/status validation on that second, authoritative locked read (the first read is untrusted — it's just a pointer lookup).

**Why this matters as an interview answer:** it's a good example of *designing for lock order as a cross-cutting invariant*, not something that lives in one file. The fix required reading and understanding three other unrelated call sites before touching a single line, and the actual code change was small — the hard part was recognizing the constraint existed at all.

---

## 2. Choosing what "cancel" means at each trip stage

**The problem.** Rider-cancellation has a clean, symmetric story: cancel = terminate, at any stage. Driver-cancellation isn't symmetric — a driver bailing *before* pickup and a driver bailing *mid-trip* are completely different situations for the rider, and conflating them into one behavior would have been wrong in either direction (auto-rebooking someone who's already in the car makes no sense; leaving a pre-pickup rider to manually rebook is worse UX than necessary).

**The decision** (made explicit before writing code, not discovered after): pre-pickup cancellation (`assigned`/`driver_arriving`) triggers automatic redispatch — reset the request back into the dispatch pool, no rider action needed. Mid-trip cancellation (`in_progress`) is terminate-only — no replacement driver can physically teleport into a moving trip, so the rider has to re-request, same as the existing rider-cancellation behavior.

**The follow-on question this raises:** does the rider get a confusing "trip cancelled" push right before a new driver shows up? Turned out no — the existing `stage` field on the (reused, unmodified) `RideCancelledV1` event already round-trips to the rider's websocket client, so a client can decide whether to show "finding a new driver" vs "trip ended" based on `stage` alone. No new event field, no `websocket-gateway` code change needed. This is the kind of thing worth checking *before* assuming a feature needs new plumbing — a lot of the apparent complexity here turned out to already be handled by a field that existed for an unrelated reason.

---

## 3. The bugs that only exist because a request can now be dispatched *twice*

This is the centerpiece. Every dispatch-related code path in this codebase — `AttemptDispatch`, the retry sweep, the whole `driver_job_offers` / `ongoing_trips` schema — was written under one unstated assumption: **a `trip_request` gets dispatched, resolved, and that's it.** Redispatch breaks that assumption for the first time in the codebase's life, and two hard-coded uniqueness constraints that had never been exercised a second time immediately surfaced as real bugs — but only when actually run end-to-end, not from reading the code.

### 3a. `driver_job_offers (request_id, driver_id)` unique index

`createOffers` did a plain `INSERT` for every dispatch attempt. That was always safe before, because the *only* way a request re-entered dispatch (the retry sweep) only happens when zero candidates were found — meaning no rows were ever created for anyone yet. Redispatch is the first path that resets a request to `searching` *after* offers already exist and partially resolved (one `accepted`, others `expired`). The second dispatch attempt tries to re-offer the same surviving driver candidate, hits the unique index, and the whole transaction rolls back.

```
ERROR: duplicate key value violates unique constraint "idx_driver_job_offers_request_driver"
```

**Fix:** `ON CONFLICT (request_id, driver_id) DO UPDATE`, resetting the existing row to a fresh pending offer, then re-querying the persisted rows before building the outbound Kafka event — because on a conflict-path upsert, the row's *actual* primary key is the pre-existing one, not the `uuid.New()` generated client-side for the insert attempt. Trusting the in-memory struct's ID here would have silently published a `job_offer_id` in the Kafka event that doesn't exist in the database — a bug that would only show up when a driver's app tried to accept an offer it was just pushed, and got a 404.

### 3b. `ongoing_trips.request_id` unique index

Once 3a was fixed, the *next* driver's `accept` call failed for the same underlying reason: `ongoing_trips` has exactly one row per `request_id`, forever, and `AcceptOffer` unconditionally `INSERT`ed a new row. The first (cancelled) driver's row already occupies that slot.

```
ERROR: duplicate key value violates unique constraint "ongoing_trips_request_id_key"
```

This one had a real design choice behind the fix, not just a mechanical upsert: should the schema's 1:1 constraint be relaxed (migration, allowing multiple `ongoing_trips` rows per request over its life), or should the existing row be reused in place? Relaxing the constraint would have rippled into every other place that assumes "the one `ongoing_trips` row for this request" — notably rider-cancellation's own lookup-by-`request_id`, which would have needed to start filtering for "the current one" instead of just taking whatever came back. Reusing the row in place needed no migration and no changes anywhere else, because `trip_history` already independently carries the full timeline (`driver_accepted` → `driver_cancelled` → `dispatch_redispatch_triggered` → `driver_accepted` again) — `ongoing_trips` was never meant to be a ledger, `trip_history` already is one. Reuse-in-place won.

### 3c. The stuck-forever request (found by watching what the *first* bug's crash actually did)

Fixing 3a and 3b would have been enough to make the happy path work, but testing the failure path (not just the fix) surfaced a third issue: `HandleDriverCancellation`'s reset-to-`searching` and its immediate `AttemptDispatch()` call are two separate transactions. When `AttemptDispatch` failed (from bug 3a, before the fix), the reset had already committed — with `next_dispatch_at` set to `nil`. The periodic retry sweep only ever picks up rows where `next_dispatch_at` is non-null and due. Result: a request that's reset to `searching` but whose one attempt to actually search failed is invisible to every recovery mechanism in the system — not retried by Kafka redelivery (the message design intentionally treats "already reset" as a no-op, for idempotency), not retried by the sweep (`next_dispatch_at IS NULL`). Permanently stuck, silently, with no error surfaced anywhere after the initial crash.

**Fix:** set `next_dispatch_at = now()` instead of `nil` during the reset. The immediate synchronous call is still there for low latency in the happy path, but the sweep is now a real safety net for the unhappy path — the same resilience story the rest of the dispatch system already relies on, rather than a special case that silently had none.

**The meta-lesson:** this bug was only found because the crash from bug 3a was left to run its natural course once — the process actually exited (by design: the fan-in error handling deliberately crashes loud rather than swallowing errors), and the *next* thing checked was "what state did that leave the data in," not just "does the fix make the demo pass." A fix that only gets verified on the happy path would have shipped bug 3c invisibly.

---

## 4. Recording a safety-relevant fact without building the feature that consumes it yet

**The ask:** when a driver cancels mid-trip, capture how far the rider actually is from their destination — a genuine passenger-safety signal — but explicitly *not* build any reactive behavior (SOS button, safety popups, route-divergence alerts) around it yet.

**The design tension:** building the data capture in a way that's actually useful later, without overbuilding the "later" part now. The answer was to keep it entirely inside the existing `trip_history` audit row — no new Kafka event field, no new consumer, nothing `websocket-gateway` needs to know about — computed from the driver's last-known GPS ping (already collected for unrelated reasons — dispatch matching) via a straight-line Haversine distance to the trip's dropoff coordinates. The formula already existed once elsewhere in the codebase (fare estimation needs pickup→dropoff distance) but in a different Go module — these are independently versioned services under a Go workspace, so it got duplicated as a ~15-line local function rather than justifying a new shared dependency for one formula. Small, deliberate duplication beats a premature shared package.

---

## 5. Excluding a driver from redispatch without adding schema, Redis, or a TTL

**The ask:** once a driver cancels, they shouldn't be immediately re-matched to the same rider's request during redispatch — a driver who just bailed shouldn't be the "fix" the system offers ten seconds later.

**The naive options considered and rejected:**
- A new `trip_requests` column tracking excluded driver IDs — a schema migration, coordinated across two repos, for something derivable from data already being written.
- A Redis-backed short-TTL exclusion key — introduces a new dependency into a service (`trip-dispatch-worker`) that has never needed Redis, for a fact that isn't actually time-boxed (the exclusion should last the *entire remaining lifetime of that specific request*, not a few minutes).

**What actually shipped:** every driver-cancellation already writes a `trip_history` row with `event_type = "driver_cancelled"`, the `request_id`, and the `driver_id` — because that's needed for the audit trail regardless. The nearest-driver query already excludes drivers with an active `ongoing_trips` row; adding one more `NOT EXISTS` subquery against `trip_history`, scoped to `request_id`, closed the gap with zero new schema, zero new infrastructure, and zero new write paths — reusing a fact the system was already recording for an unrelated reason.

---

## Recurring theme across all of this

The pattern that shows up in almost every section above: **the hard part was rarely writing new code — it was recognizing an existing invariant (a lock order, a uniqueness constraint, an event field, an audit table) that a new feature was about to violate or could be reused instead of duplicated.** Three of the five sections above ended with "no schema change / no new event field / no new dependency needed" specifically because the right fix was to look harder at what the system already tracked, not to add a mechanism for tracking it again.
