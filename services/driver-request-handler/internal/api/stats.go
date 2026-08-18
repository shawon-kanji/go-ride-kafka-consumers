package api

import (
	"encoding/json"
	"log"
	"net/http"

	"go-ride-kafka-consumers/services/driver-request-handler/internal/auth"

	"github.com/google/uuid"
)

type driverStatsResponse struct {
	RatingAverage   *float64 `json:"rating_average,omitempty"`
	RatingCount     int      `json:"rating_count"`
	TripCount       int      `json:"trip_count"`
	AcceptanceScore *float64 `json:"acceptance_score,omitempty"`
}

// handleDriverStats backs D11's three header tiles (rating, trip count,
// acceptance score) and D10's "frequent cancellations affect the
// acceptance score" warning.
func (s *Server) handleDriverStats(w http.ResponseWriter, r *http.Request) {
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

	row, err := s.offers.DriverStats(r.Context(), driverID)
	if err != nil {
		log.Printf("driver stats driver_id=%s: %v", driverID, err)
		writeJSONError(w, http.StatusInternalServerError, "internal_error", "failed to load driver stats")
		return
	}

	response := driverStatsResponse{
		RatingAverage:   row.RatingAverage,
		RatingCount:     row.RatingCount,
		TripCount:       row.TripCount,
		AcceptanceScore: computeAcceptanceScore(row.AcceptedOffers, row.DriverCancelledTrips, row.TotalScoredOffers),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// computeAcceptanceScore is the pure formula, split out for unit testing:
// nil when there's no data yet (a brand-new driver shouldn't show a
// misleading 0%, and the client renders that as "not enough data" rather
// than a bad score), else 100 * (accepted - driverCancelled) / total,
// clamped to [0, 100] so a driver who cancels every trip they accept can't
// push the score negative.
func computeAcceptanceScore(accepted, driverCancelled, total int) *float64 {
	if total == 0 {
		return nil
	}
	score := 100 * float64(accepted-driverCancelled) / float64(total)
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	return &score
}
