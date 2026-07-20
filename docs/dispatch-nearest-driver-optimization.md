# Optimize nearest-driver dispatch query using S2 spatial pre-filtering

## Context

`internal/dispatch/service.go`'s `nearestDriversQuery` (in `services/trip-dispatch-worker`) currently does a **full-table scan** over `driver_locations`, computing a Haversine trig expression (`acos`/`cos`/`sin`) for every fresh row on every single dispatch attempt (and every retry, and every sweep tick), before filtering by radius. There is no spatial index — `driver_locations` only has plain btree indexes on `geohash`, `s2_cell_id`, and `recorded_at` (confirmed in `go-ride-db-schema/migrations/000004_create_driver_locations.up.sql`), none of which currently narrow the search by location. This is fine at toy data volumes but doesn't scale as the driver fleet grows.

The question was whether the stored `geohash` column could be used to optimize this. On investigation, that column turns out to be the *wrong* one to use directly:

- `driver_locations.geohash` = `s2Cell(lat,lng).Parent(16).ToToken()` — an S2 cell **token** at a fixed level 16 (~0.3–1km² cells), stored as a **trimmed hex string** (`ToToken()` strips trailing zero nibbles for compactness). Trimmed, variable-width hex strings are *not* safe for prefix/range matching, and level 16 is far too fine-grained to cover a 20–30km search radius (would require thousands of exact-match tokens).
- `driver_locations.s2_cell_id` = `strconv.FormatUint(uint64(cell), 10)` — the **leaf-level (level 30) S2 cell ID**, stored as a plain decimal string. This is the column S2's own indexing trick is designed around: for *any* coarser covering cell `C` (at any level), `C.RangeMin()`/`C.RangeMax()` return the leaf-level ID bounds of every possible descendant of `C` — confirmed directly from the `golang/geo` source (`s2/cellid.go:323-327`) already vendored in this workspace (`github.com/golang/geo v0.0.0-20260713102120-857a528af641`, already a dependency of `cab-request-handler`/`location-producers`). So a numeric `BETWEEN RangeMin AND RangeMax` on `s2_cell_id` correctly and efficiently narrows to "all driver points inside this covering cell," for a covering computed at whatever level fits the current search radius.

The blocker: `s2_cell_id` is stored as `VARCHAR(32)`, so `BETWEEN` on it today is a lexicographic *string* comparison, not numeric — incorrect for ranges. It needs to become a numeric column. Note `uint64` values here can exceed Postgres's signed `BIGINT` max (`2^63-1`), so the target type must be `NUMERIC(20,0)`, not `BIGINT`.

## Approach

1. **Schema migration** (`go-ride-db-schema`, new `000015_convert_s2_cell_id_to_numeric`):
   - `ALTER TABLE driver_locations ALTER COLUMN s2_cell_id TYPE NUMERIC(20,0) USING s2_cell_id::numeric;` (existing values are always valid decimal strings from `FormatUint`, so the cast is safe). Postgres rebuilds the existing btree index automatically as part of the type change.
   - Down migration casts back: `... TYPE VARCHAR(32) USING s2_cell_id::text;` (numeric→text round-trips exactly for a scale-0 numeric).
   - No Go struct field type change needed — `models.DriverLocation.S2CellID` stays `string` (gorm tag comment updated to `type:numeric(20,0)`); nothing else reads/writes this column differently, so this is purely a schema-side tightening. Precedent: additive/non-breaking changes have been patch bumps in this repo (v0.2.1, v0.3.1) — tag this as **v0.3.2**.
   - `geohash` column is untouched — out of scope, not the right tool here.

2. **`trip-dispatch-worker` go.mod**: add `github.com/golang/geo v0.0.0-20260713102120-857a528af641` (same pinned version already used elsewhere in this workspace — no version drift).

3. **New helper in `internal/dispatch`** (e.g. `covering.go`): given `(lat, lng, radiusKM)`, compute an S2 covering and return parallel slices of decimal-string `rangeMin`/`rangeMax` pairs:
   ```go
   const earthRadiusKM = 6371.0 // match haversineKM's existing constant convention

   func s2CoveringRanges(lat, lng, radiusKM float64) (mins, maxs []string) {
       center := s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng))
       angle := s1.Angle(radiusKM / earthRadiusKM) // radians; no public km->angle helper exists in this library version
       cap := s2.CapFromCenterAngle(center, angle)
       coverer := &s2.RegionCoverer{MinLevel: 0, MaxLevel: s2.MaxLevel, MaxCells: 8} // 8 = library's own default
       for _, cellID := range coverer.Covering(cap) {
           mins = append(mins, strconv.FormatUint(uint64(cellID.RangeMin()), 10))
           maxs = append(maxs, strconv.FormatUint(uint64(cellID.RangeMax()), 10))
       }
       return mins, maxs
   }
   ```
   `RegionCoverer.Covering` adaptively picks cell levels to hit ~`MaxCells` cells regardless of radius — no manual level-vs-radius tuning needed, confirmed via source (`s2/regioncoverer.go`).

4. **Rewrite `nearestDriversQuery` construction** in `service.go` to build the `s2_cell_id BETWEEN ? AND ?` OR-block dynamically (via `strings.Builder`), one pair per covering cell (typically ≤8 clauses — small, bounded, no array/unnest binding needed, avoiding any ambiguity in how GORM's `Raw` handles slice args):
   ```sql
   WITH candidate_distances AS (
       SELECT dl.driver_id,
           (6371 * acos(LEAST(1, GREATEST(-1,
               cos(radians(?)) * cos(radians(dl.latitude)) * cos(radians(dl.longitude) - radians(?)) +
               sin(radians(?)) * sin(radians(dl.latitude))
           )))) AS distance_km
       FROM driver_locations dl
       WHERE dl.recorded_at >= ?
         AND ( (dl.s2_cell_id BETWEEN ? AND ?) OR (dl.s2_cell_id BETWEEN ? AND ?) /* ...one pair per covering cell */ )
   )
   SELECT cd.driver_id, cd.distance_km FROM candidate_distances cd
   WHERE cd.distance_km <= ?
     AND NOT EXISTS (SELECT 1 FROM ongoing_trips ot WHERE ot.driver_id = cd.driver_id AND ot.status IN (?))
   ORDER BY cd.distance_km ASC LIMIT ?
   ```
   The `BETWEEN` pairs now hit the (now-numeric) indexed `s2_cell_id` column with a real range scan, filtering the row set *before* any trig is computed — Haversine still runs (and the exact `distance_km <= radius` filter still applies) only on that much smaller candidate set, since the S2 covering is an over-approximation and must not be trusted as the final distance filter. Recomputed fresh on every attempt (initial + each retry), since radius changes across retries (20→25→30km).

5. **`AttemptDispatch`**: call `s2CoveringRanges(req.PickupLat, req.PickupLng, radius)` before running the query, pass the resulting args through in the same order as the dynamically-built SQL placeholders.

## Files touched
- `go-ride-db-schema/migrations/000015_convert_s2_cell_id_to_numeric.{up,down}.sql` — new
- `go-ride-db-schema/models/driver_location.go` — gorm tag comment update only (no field type change)
- `go-ride-kafka-consumers/services/trip-dispatch-worker/go.mod` — add `golang/geo`
- `go-ride-kafka-consumers/services/trip-dispatch-worker/internal/dispatch/covering.go` — new
- `go-ride-kafka-consumers/services/trip-dispatch-worker/internal/dispatch/service.go` — query construction + `AttemptDispatch` call site

## Verification
1. Migration: run `migrate up`/`down`/`up` against local Postgres, confirm `\d driver_locations` shows `s2_cell_id` as `numeric(20,0)` and the existing index survives/rebuilds.
2. Re-seed the same test drivers/locations used in the earlier Phase 5 verification; re-run the happy-path test (`/fare-estimate` → `/request-cab`) and confirm identical results (same 4 offers, same distance ordering) as before — this change must be behavior-neutral, only a performance improvement.
3. Add a temporary `EXPLAIN ANALYZE` check (manual, not committed) on the query before/after to confirm the new query uses an index range scan (`Index Scan`/`Bitmap Index Scan` on `s2_cell_id`) instead of a full `Seq Scan` on `driver_locations`.
4. Re-run the zero-driver/retry/timeout path test to confirm it's unaffected (still grows radius 20→25→30, still times out after 5 attempts).
5. Bump `go-ride-db-schema` to `v0.3.2` in all four consumer modules (root, `location-consumers`, `cab-request-handler`, `trip-dispatch-worker`) as done for prior tags.
