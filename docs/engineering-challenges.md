# Engineering Challenges Across the Project

Project-wide hard scenarios and how they were solved. Driver
cancellation gets a full write-up of its own in
`docs/driver-cancellation-engineering-challenges.md`; it's summarized briefly
here for completeness and skipped in depth to avoid repeating it.


---

## 1. Geospatial nearest-driver search with no PostGIS, no GiST index

**The problem.** Finding the nearest available drivers to a pickup point is
the core of the whole dispatch system, and the schema has no PostGIS
extension, no geography column, no spatial index — just plain
`latitude`/`longitude` doubles. The first working version of this query did
exactly what that constraint suggests: a full Haversine trig expression
(`acos`/`cos`/`sin`) computed against *every fresh row* in `driver_locations`,
on every dispatch attempt, every retry, every sweep tick. Correct, but it's a
full table scan that gets worse linearly with fleet size — fine at demo
scale, a real problem once the driver count grows.

**The tempting shortcut that was actually wrong.** `driver_locations` already
stores a `geohash` column, updated on every location ping — the obvious thing
to reach for. Looking closer at how it's actually computed
(`s2Cell(lat,lng).Parent(16).ToToken()`) revealed two independent reasons it
doesn't work: it's a fixed S2 level-16 cell (~0.3–1km² each — far too fine to
cover a 20–30km search radius without needing thousands of exact-match
tokens), and `ToToken()` trims trailing zero nibbles for compactness, which
means the stored strings are *variable-width hex* — not safe for prefix or
range matching at all. Shipping a "spatial index" on the wrong column would
have been a change that looked like an optimization, passed a quick smoke
test, and either did nothing or returned wrong results at the edges.

**The actual mechanism.** The *other* stored column, `s2_cell_id`, is the
leaf-level (level 30) S2 cell ID as a plain decimal number. Reading the S2
library's own source directly (`s2/cellid.go`) confirmed the trick it's built
around: for any coarser covering cell at any level, `RangeMin()`/`RangeMax()`
return the leaf-level ID bounds of *every possible descendant* of that cell.
So a numeric `BETWEEN RangeMin AND RangeMax` on `s2_cell_id` is a correct,
efficient "is this point inside this covering cell" filter, for a covering
computed at whatever level fits the search radius —
`s2.RegionCoverer{MaxCells: 8}.Covering(cap)` adaptively picks the level,
no manual radius-to-level tuning needed.

**The blocker that almost sank it anyway.** `s2_cell_id` was stored as
`VARCHAR(32)`. A `BETWEEN` on a string column is a *lexicographic* string
comparison, not numeric — silently wrong for range queries (`"9" > "10"` as
strings). Fixing this needed a migration to `NUMERIC(20,0)` — not `BIGINT`,
because `uint64` cell IDs can exceed Postgres's signed `bigint` max
(`2^63-1`). Getting the column type right mattered more than the query logic
around it; the query would have "worked" against a string column too, just
returned the wrong candidate set some of the time, in a way that's very easy
to not notice in a small test.

**Result:** the S2 covering pre-filters on an indexed range scan before any
trig runs at all; the exact Haversine `distance_km <= radius` check still
applies afterward, since the covering is an over-approximation, never trusted
as the final answer. Same nearest-driver ordering as before, verified as a
behavior-neutral change — this was a performance fix, not a logic change, and
proving that distinction mattered as much as making it fast.

---

## 2. Delivering a realtime push when you don't know which server holds the socket

**The problem.** `websocket-gateway` needs to run as multiple stateless-ish
instances (e.g. behind a k8s Service, no sticky sessions), but a live
WebSocket connection is inherently pinned to exactly one process's memory —
there's no way to hand a TCP connection to another pod. Every event that
needs to reach a driver or rider (job offers, assignments, trip state
changes, live location) is produced by a *different* service (dispatch
worker, driver-request-handler) that has no idea which gateway instance, if
any, is holding that user's connection — and the Kafka consumer that picks up
the event could itself be running on any instance.

**The fix.** Broadcast, then locally filter. Every Kafka event a gateway
consumer picks up gets republished once to a Redis pub/sub channel; *every*
gateway instance subscribes to *every* channel, receives *every* message, and
independently does a cheap local hub lookup (`driver_id`/`rider_id` →
in-memory connection map) — only the one instance that actually holds the
target connection does anything with it. This trades "some redundant
delivery-attempt work on every idle instance" for "zero shared state, zero
sticky sessions, zero need for the producer to know anything about connection
topology." It's the same shape six independent times in this codebase (job
offers, assignments, trip start/end/complete, cancellation), which is itself
worth noting — once the pattern was right once, every subsequent realtime
feature was a copy of the same three-file shape (`broadcast.go` /
`notifier.go` / `deliver.go`), not a new design problem.

---

## 3. Filtering a firehose *before* fan-out, not after

**The problem.** The broadcast-then-filter pattern above works fine at the
volume of job offers or assignments (one event per dispatch attempt, maybe
per second across the whole fleet). Driver location pings are a completely
different volume class — continuous, per-driver, regardless of whether that
driver is idle or mid-trip. Naively reusing the same "publish every event to
Redis, let every instance filter locally" pattern here would mean every
gateway pod receiving a Redis message for *every idle driver's every ping*,
fleet-wide, forever — the filtering was happening at the wrong end of the
pipe.

**The fix.** Move the filter before the fan-out, not after. A separate
Redis-backed KV store (`presence.Store`, distinct from the pub/sub bus)
tracks `driver_id → {rider_id, trip_id}` only for drivers *currently matched
to an active trip* (written at assignment time, cleared at trip-start — a
location ping after pickup adds nothing, the rider's already in the car).
The consumer does one cheap KV lookup per ping; on a miss — the large
majority, since most drivers are idle — it's an immediate no-op with no
Redis publish and deliberately no log line (would spam at this volume). Only
a hit gets broadcast at all. Same overall shape as everything else in the
gateway, just with the expensive "is this relevant to anyone" check moved to
before the fan-out instead of after it.

---

## 4. Letting a rider see a price before committing to a search, without breaking the schema's assumptions

**The problem.** The original design created a fare and a trip request
atomically, in one call — simple, but it meant a rider couldn't see a price
and decide whether to actually book. Splitting this into "get a quote" then
"book against that quote" sounds like a small API change, but the schema
had `trip_fares.request_id` as `NOT NULL UNIQUE` with a foreign key *to*
`trip_requests` — i.e., a fare was only ever allowed to exist as a child of
a request that already existed. The new flow needed the exact inverse: a
fare that exists on its own, then gets claimed by a request afterward.

**The fix that avoided a bigger migration.** Rather than restructuring the
relationship, just drop `NOT NULL` — Postgres's `UNIQUE` constraint already
allows multiple `NULL`s, so `request_id IS NULL` cleanly means "unconsumed,
still bookable," and `IS NOT NULL` means "already booked," with the same FK
and uniqueness guarantees intact. Booking becomes a single conditional
`UPDATE trip_fares SET request_id = ? WHERE fare_id = ? AND request_id IS NULL`
inside the same transaction as the `trip_requests` insert, checking
`RowsAffected == 1`. That one check *is* the entire "first booker wins"
race guard for two concurrent booking attempts against the same quote — no
explicit row lock needed, because the conditional update's atomicity is the
lock. Simpler and cheaper than it would have been to reason about a
`SELECT ... FOR UPDATE` here, and it composes cleanly with the fare's own
expiry check (also just a `WHERE` clause).

---

## 5. First-wins acceptance under real concurrency

**The problem.** A dispatch attempt offers the same trip to multiple nearby
drivers simultaneously (by design — whoever accepts first wins, the rest
lose). That means the accept endpoint has to correctly handle N drivers all
hitting "accept" for overlapping offers on the same request within
milliseconds of each other, and guarantee exactly one of them succeeds no
matter what order their requests actually arrive at the database in.

**The fix.** Row-lock the specific offer being accepted first
(`driver_job_offers` `FOR UPDATE`, verifying it's still `pending`,
unexpired, and actually belongs to the calling driver), *then* row-lock the
parent `trip_requests` row and check it's still `offered` — under that
second lock, not before it. A driver who wins the race on their own offer
row can still lose if someone else's transaction already flipped the parent
request to `assigned` a moment earlier; that second check is what makes it
correct, not just "lock something." Losing drivers get a clean `409` rather
than a confusing success, and this exact lock-order pattern (child row,
then parent row, re-validating state at each step) became the template every
other locking flow in the codebase was built to match or deliberately
diverge from — see the driver-cancellation write-up for what happens when
a new flow needs the *opposite* order and why that has to be handled
carefully, not copied blindly.

---

## 6. Living with at-least-once delivery and no cross-topic ordering guarantee

**The problem.** Six independent Kafka topics feed six independent consumers
in `websocket-gateway`, each mapping to its own realtime push. Kafka gives
no ordering guarantee *across* topics, only within one, and consumers can
reprocess a message after a crash-before-commit. Two very real
consequences: a `driver.location.updated.v1` ping can arrive and be
processed *before* the `ride.assigned.v1` event for the same trip has been
handled (two different topics, two different consumer goroutines, no
coordination between them) — and any consumer can, in principle, see the
same message twice.

**The decision, made explicitly rather than discovered as a bug later.**
Don't engineer around either problem — design for them to be harmless
instead. Every producer commits its database write *before* publishing to
Kafka, never the reverse, so a push can never describe state that isn't
durably true yet. A location ping that arrives before its trip's assignment
has been processed simply misses the presence-store lookup (empty result,
not an error) and gets silently dropped — the next ping a few seconds later
finds the mapping now in place and starts flowing normally. Nothing retries
it, nothing alerts on it, because nothing needs to: it self-heals by
volume. Malformed/undecodable messages get the opposite treatment
deliberately — those get flagged as poison pills (a distinct
`invalidMessageError` type), logged, and their offset committed anyway so
they don't wedge the whole consumer forever, rather than trying to be clever
about repair. And for the one flow that genuinely can't tolerate silent
loss — job offers, which a driver must be able to act on — durability comes
from a completely different mechanism: reconnect replay queries Postgres
directly for pending, unexpired offers on every WebSocket upgrade, so the
database (not any in-memory queue or the Kafka log) is the actual source of
truth a client falls back to. Three different consistency guarantees,
chosen deliberately per use case, rather than one blanket policy applied
everywhere.

---

## 7. Keeping N independently-versioned services in sync with one shared schema

**The problem.** `go-ride-db-schema` is consumed as a tagged Go module by up
to six different `go.mod` files across two repos (this one and
`go-ride-backend`), each pinned to its own version. Every schema change
means: migrate, tag a release, then remember to bump the dependency in
*every consumer that needs it* — and "needs it" isn't always obvious, since
a service that doesn't touch a new column directly can still need the bump
just to get a struct field it reads elsewhere, or to pick up a new shared
constant.

**Where this actually bit, concretely:** partway through the
driver-cancellation work, `driver-request-handler` and `trip-dispatch-worker`
were discovered still pinned to an older schema tag than `cab-request-handler`
— the earlier rider-cancellation feature had only bumped the one service
that needed it *at the time*, and nothing forced the other two to catch up
until they needed the same struct fields months of feature-work later. Not a
bug exactly (each service worked fine on its own pinned version), but exactly
the kind of drift that's invisible until the moment two services need to
agree on a shared contract and one of them is quietly behind.

**What keeps it manageable rather than chaotic:** a strict convention,
enforced by habit rather than tooling — the dependency bump is always its
own commit, separate from the feature code that needs it, and additive
schema changes are patch-version bumps while breaking Go-struct changes are
minor/major bumps. That convention doesn't prevent drift, but it makes drift
easy to *find* (`grep` every `go.mod` for the schema version is a one-line
audit) and easy to fix in isolation, without tangling a dependency bump into
the diff of unrelated feature work. The real lesson: in a polyrepo/versioned-
module setup, the process discipline around *how* a shared dependency gets
bumped matters as much as the schema design itself.

---

## 8. Driver-initiated cancellation and redispatch (summary — see the dedicated doc)

Full write-up: `docs/driver-cancellation-engineering-challenges.md`.

Short version: adding driver-side cancellation required matching an
existing lock-order invariant from a different angle (URL only gives an
`ongoing_trip_id`, but the safe order demands locking `trip_requests`
first), and choosing per-stage semantics (auto-redispatch before pickup,
terminate-only mid-trip) that reused an existing event field instead of
needing new plumbing. The most valuable part of that work, though, was what
end-to-end testing surfaced rather than what code review would have caught:
letting a request be dispatched a *second* time broke two uniqueness
constraints (`driver_job_offers (request_id, driver_id)`,
`ongoing_trips.request_id`) that had implicitly never been exercised twice
in the codebase's life, plus a "stuck forever with no recovery path" bug
that only existed because a synchronous retry call had no sweep-based
safety net if it failed. None of the three were visible from reading the
new code in isolation — they only existed at the intersection of new
behavior and old, unstated assumptions elsewhere in the system.

---

## 9. Known gap: `presence.Store` has no disaster-recovery path if Redis loses it

**Not fixed — recorded here as a known gap and future improvement**, surfaced
while discussing what actually survives a `websocket-gateway` pod crash.

**What survives a pod crash vs. what doesn't.** `ws.Hub` (the in-memory
`driver_id → device_id → connection` registry, `internal/ws/driver_hub.go`)
is trivially and completely wiped when a gateway pod crashes — but that's not
a gap, it's expected and harmless: a live WebSocket's underlying TCP
connection dies with the process regardless of where it's tracked, the
client's own reconnect logic opens a fresh socket against whichever pod it
lands on next, and `internal/offers.Replayer` already queries Postgres for
pending, unexpired job offers on every upgrade — reconnect and first-connect
are handled identically by design.

`presence.Store` (the Redis-backed `driver_id → active trip` map that filters
the driver-location firehose down to only drivers currently mid-trip,
`internal/presence/store.go`) is *not* lost when a gateway pod crashes either
— it was deliberately externalized to Redis, a separate service from the
gateway pods, for exactly that reason.

**The actual gap** is one level further down: if *Redis itself* loses that
key — a restart without persistence enabled, a failover, an outage — nothing
rebuilds it. It's written exactly once, at assignment time
(`internal/assignments/notifier.go`'s `HandleRideAssignedEvent`), and cleared
exactly twice, at trip-start and at cancellation. There is no path that
re-derives it from Postgres, even though Postgres already holds the
authoritative answer (`ongoing_trips.status`). A Redis-side data-loss event
today means live `driver_location` pushes silently stop for the affected
trips — a Redis miss looks identical to "driver not currently on a trip," so
it fails with no error and no log — until either trip-start clears the key
anyway (moot at that point) or the 2-hour TTL lapses. Bounded, not
catastrophic: `/current-trip` polling still covers every other piece of trip
state for the rider, this only affects the live location tick specifically.
But it's silent and currently unrecoverable within that window.

**Shape of a future fix, not built:** a periodic reconciliation sweep inside
`websocket-gateway`, mirroring the ticker-loop shape
`trip-dispatch-worker/internal/worker/sweep_runner.go` already uses for its
own safety-net loop — on each tick, query
`ongoing_trips WHERE status IN (assigned, driver_arriving)` (deliberately
*not* the existing `schemamodels.ActiveOngoingTripStatuses()` helper, which
also includes `in_progress`/`awaiting_payment` — states where the entry was
just deliberately cleared and must stay cleared) and re-assert each row into
`presence.Store` via the same `SetActiveTrip` call `assignments.Notifier`
already makes. Since that call is an unconditional `SET ... EX`, this is a
plain idempotent upsert-and-refresh — no delete/merge logic needed, and safe
to run redundantly on every gateway instance with no leader election, the
same "cheap redundant work over added coordination" preference the rest of
this service's fan-out design already leans on. Would need to be wired as a
non-critical, fire-and-forget goroutine (like the existing `sweepRunner` in
`trip-dispatch-worker`), not folded into the gateway's critical-path
`errCh` fan-in — reconciliation is a best-effort self-heal, not a delivery
guarantee, and a transient DB blip during one tick must not be allowed to
crash live WebSocket connections.

---

## Recurring theme

Nearly every section above is the same shape: something that looked like
the obvious tool for the job (a stored geohash column, copying an existing
fan-out pattern, a straightforward `INSERT`) turned out to be wrong once
checked against how it was *actually* built, or against an assumption baked
in elsewhere that a new feature was the first thing to ever violate. The
individual fixes were almost always small. Finding out *which* assumption
was about to break was the actual work.
