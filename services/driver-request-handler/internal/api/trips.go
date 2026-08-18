package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go-ride-kafka-consumers/services/driver-request-handler/internal/auth"

	"github.com/google/uuid"
)

const (
	defaultTripsPageSize = 20
	maxTripsPageSize     = 50
)

type tripHistoryEntry struct {
	RequestID    string     `json:"request_id"`
	TripID       string     `json:"trip_id"`
	RiderID      string     `json:"rider_id"`
	Status       string     `json:"status"`
	PickupLat    float64    `json:"pickup_lat"`
	PickupLng    float64    `json:"pickup_lng"`
	DropoffLat   float64    `json:"dropoff_lat"`
	DropoffLng   float64    `json:"dropoff_lng"`
	AssignedAt   time.Time  `json:"assigned_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	CancelledAt  *time.Time `json:"cancelled_at,omitempty"`
	FinalFare    *float64   `json:"final_fare,omitempty"`
	CurrencyCode *string    `json:"currency_code,omitempty"`
}

type tripHistoryResponse struct {
	Trips      []tripHistoryEntry `json:"trips"`
	NextCursor string             `json:"next_cursor,omitempty"`
}

func (s *Server) handleListTrips(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	token := bearerToken(r)
	if token == "" {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "missing bearer token")
		return
	}

	claims, err := s.verifier.Parse(token)
	if err != nil || claims.Role != auth.DriverRole {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid or non-driver token")
		return
	}

	driverID, err := uuid.Parse(claims.UserID)
	if err != nil {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid driver id in token")
		return
	}

	limit, err := parseTripsLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_limit", err.Error())
		return
	}

	cursorTime, cursorID, hasCursor, err := parseTripsCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_cursor", "cursor is invalid")
		return
	}

	// Overfetch by one so a full-looking page can be told apart from a true
	// last page — otherwise a page that happens to end exactly on `limit`
	// always emits a next_cursor whose page then comes back empty.
	rows, err := s.offers.ListTrips(r.Context(), driverID, cursorTime, cursorID, hasCursor, limit+1)
	if err != nil {
		log.Printf("list trips driver_id=%s: %v", driverID, err)
		writeJSONError(w, http.StatusInternalServerError, "internal_error", "failed to load trip history")
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
			RiderID:     row.RiderID.String(),
			Status:      row.Status,
			PickupLat:   row.PickupLat,
			PickupLng:   row.PickupLng,
			DropoffLat:  row.DropoffLat,
			DropoffLng:  row.DropoffLng,
			AssignedAt:  row.AssignedAt,
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
		response.Trips = append(response.Trips, entry)
	}
	if hasMore {
		last := rows[len(rows)-1]
		response.NextCursor = encodeTripsCursor(last.AssignedAt, last.OngoingTripID)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
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
