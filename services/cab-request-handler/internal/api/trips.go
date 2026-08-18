package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
)

const (
	tripsRoute           = apiPrefix + "/trips"
	defaultTripsPageSize = 20
	maxTripsPageSize     = 50
)

// terminalTripRequestStatuses are the trip_requests statuses that mean a
// trip request never reached ongoing_trips at all (cancelled or timed out
// during search) — these rows are their own complete trip history entry.
// Once a request is assigned, trip_requests.status freezes at "assigned"
// forever; ongoing_trips becomes the sole source of truth for anything
// past that point (see driver-request-handler's EndTrip/CollectPayment,
// which never touch trip_requests), so those trips are found via the
// ongoing_trips side of the join below instead.
var terminalTripRequestStatuses = []string{
	schemamodels.TripRequestStatusCancelled,
	schemamodels.TripRequestStatusTimedOut,
}

var terminalOngoingTripStatuses = []string{
	schemamodels.OngoingTripStatusCompleted,
	schemamodels.OngoingTripStatusCancelled,
}

type tripHistoryEntry struct {
	RequestID    string     `json:"request_id"`
	TripID       string     `json:"trip_id"`
	Status       string     `json:"status"`
	PickupLat    float64    `json:"pickup_lat"`
	PickupLng    float64    `json:"pickup_lng"`
	DropoffLat   float64    `json:"dropoff_lat"`
	DropoffLng   float64    `json:"dropoff_lng"`
	RequestedAt  time.Time  `json:"requested_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	CancelledAt  *time.Time `json:"cancelled_at,omitempty"`
	DriverID     *string    `json:"driver_id,omitempty"`
	FinalFare    *float64   `json:"final_fare,omitempty"`
	CurrencyCode *string    `json:"currency_code,omitempty"`
}

type tripHistoryResponse struct {
	Trips      []tripHistoryEntry `json:"trips"`
	NextCursor string             `json:"next_cursor,omitempty"`
}

// tripHistoryRow is the Scan target for loadRiderTripHistory's raw query —
// explicit column tags rather than relying on snake_case inference, matching
// trip-dispatch-worker's own driverCandidate precedent for Raw().Scan().
type tripHistoryRow struct {
	RequestID    uuid.UUID  `gorm:"column:request_id"`
	TripID       uuid.UUID  `gorm:"column:trip_id"`
	Status       string     `gorm:"column:status"`
	PickupLat    float64    `gorm:"column:pickup_lat"`
	PickupLng    float64    `gorm:"column:pickup_lng"`
	DropoffLat   float64    `gorm:"column:dropoff_lat"`
	DropoffLng   float64    `gorm:"column:dropoff_lng"`
	RequestedAt  time.Time  `gorm:"column:requested_at"`
	CompletedAt  *time.Time `gorm:"column:completed_at"`
	CancelledAt  *time.Time `gorm:"column:cancelled_at"`
	DriverID     *uuid.UUID `gorm:"column:driver_id"`
	FinalFare    *float64   `gorm:"column:final_fare"`
	CurrencyCode *string    `gorm:"column:currency_code"`
}

func (s *Server) handleListTrips(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	riderID, ok := s.authenticateRider(w, r)
	if !ok {
		return
	}

	limit, err := parseTripsLimit(r.URL.Query().Get("limit"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cursorTime, cursorID, hasCursor, err := parseTripsCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		http.Error(w, "cursor is invalid", http.StatusBadRequest)
		return
	}

	// Overfetch by one so a full-looking page can be told apart from a true
	// last page — otherwise a page that happens to end exactly on `limit`
	// always emits a next_cursor whose page then comes back empty.
	rows, err := s.loadRiderTripHistory(r.Context(), riderID, cursorTime, cursorID, hasCursor, limit+1)
	if err != nil {
		log.Printf("list trips rider_id=%s: %v", riderID, err)
		http.Error(w, "failed to load trip history", http.StatusInternalServerError)
		return
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}

	response := tripHistoryResponse{Trips: make([]tripHistoryEntry, 0, len(rows))}
	for _, row := range rows {
		entry := tripHistoryEntry{
			RequestID:   row.RequestID.String(),
			TripID:      row.TripID.String(),
			Status:      row.Status,
			PickupLat:   row.PickupLat,
			PickupLng:   row.PickupLng,
			DropoffLat:  row.DropoffLat,
			DropoffLng:  row.DropoffLng,
			RequestedAt: row.RequestedAt,
			CompletedAt: row.CompletedAt,
			CancelledAt: row.CancelledAt,
			FinalFare:   row.FinalFare,
		}
		// A cancelled trip's final_fare is always nil — cash is only ever
		// charged at EndTrip, which a cancelled trip never reaches — so
		// only surface the quote's currency alongside an actual amount, not
		// as a dangling currency with nothing to display next to it.
		if row.FinalFare != nil {
			entry.CurrencyCode = row.CurrencyCode
		}
		if row.DriverID != nil {
			driverID := row.DriverID.String()
			entry.DriverID = &driverID
		}
		response.Trips = append(response.Trips, entry)
	}
	if hasMore {
		last := rows[len(rows)-1]
		response.NextCursor = encodeTripsCursor(last.RequestedAt, last.RequestID)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// loadRiderTripHistory returns riderID's terminal trips (cancelled/timed-out
// during search, or completed/cancelled after assignment) newest first,
// keyset-paginated on (requested_at, request_id) rather than OFFSET so a
// page boundary never shifts under concurrent inserts.
func (s *Server) loadRiderTripHistory(ctx context.Context, riderID uuid.UUID, cursorTime time.Time, cursorID uuid.UUID, hasCursor bool, limit int) ([]tripHistoryRow, error) {
	query := `
		SELECT
			tr.request_id, tr.trip_id, tr.pickup_lat, tr.pickup_lng, tr.dropoff_lat, tr.dropoff_lng,
			tr.requested_at,
			COALESCE(ot.status, tr.status) AS status,
			ot.driver_id, ot.completed_at, ot.cancelled_at, ot.final_fare,
			tf.currency_code
		FROM trip_requests tr
		LEFT JOIN ongoing_trips ot ON ot.request_id = tr.request_id
		LEFT JOIN trip_fares tf ON tf.fare_id = tr.fare_id
		WHERE tr.rider_id = ?
		  AND (
		    (ot.request_id IS NULL AND tr.status IN (?))
		    OR ot.status IN (?)
		  )
	`
	args := []any{riderID, terminalTripRequestStatuses, terminalOngoingTripStatuses}
	if hasCursor {
		query += " AND (tr.requested_at, tr.request_id) < (?, ?)"
		args = append(args, cursorTime, cursorID)
	}
	query += " ORDER BY tr.requested_at DESC, tr.request_id DESC LIMIT ?"
	args = append(args, limit)

	var rows []tripHistoryRow
	if err := s.db.WithContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("query rider trip history: %w", err)
	}
	return rows, nil
}

func parseTripsLimit(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultTripsPageSize, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("limit must be a positive integer")
	}
	if parsed > maxTripsPageSize {
		parsed = maxTripsPageSize
	}
	return parsed, nil
}

// parseTripsCursor decodes an opaque cursor from encodeTripsCursor. An empty
// input is not an error — it just means "first page" (hasCursor=false).
func parseTripsCursor(raw string) (t time.Time, id uuid.UUID, hasCursor bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, uuid.UUID{}, false, nil
	}

	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return time.Time{}, uuid.UUID{}, false, fmt.Errorf("decode cursor: %w", err)
	}
	parts := strings.SplitN(string(decoded), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, uuid.UUID{}, false, fmt.Errorf("malformed cursor")
	}
	t, err = time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, uuid.UUID{}, false, fmt.Errorf("parse cursor time: %w", err)
	}
	id, err = uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, uuid.UUID{}, false, fmt.Errorf("parse cursor id: %w", err)
	}
	return t, id, true, nil
}

func encodeTripsCursor(t time.Time, id uuid.UUID) string {
	raw := fmt.Sprintf("%s|%s", t.Format(time.RFC3339Nano), id.String())
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}
