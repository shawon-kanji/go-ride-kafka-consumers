package api

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"go-ride-kafka-consumers/services/driver-request-handler/internal/auth"
	"go-ride-kafka-consumers/services/driver-request-handler/internal/offers"

	"github.com/google/uuid"
)

type onlineTimeDailyEntry struct {
	Date          string  `json:"date"` // YYYY-MM-DD, UTC calendar day
	OnlineMinutes float64 `json:"online_minutes"`
}

type onlineTimeResponse struct {
	Period       string                 `json:"period"`
	TotalMinutes float64                `json:"total_minutes"`
	Daily        []onlineTimeDailyEntry `json:"daily,omitempty"`
}

// handleOnlineTime backs D06's second stat card: elapsed online time,
// derived from driver_online_sessions (opened/closed by go-ride-backend's
// online toggle — see that repo's DriverRepositoryGorm.UpdateOnlineStatus).
func (s *Server) handleOnlineTime(w http.ResponseWriter, r *http.Request) {
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

	now := time.Now().UTC()
	period, since, windowDays, ok := parsePeriodWindow(w, r, now)
	if !ok {
		return
	}

	rows, err := s.offers.OnlineSessionsOverlapping(r.Context(), driverID, since, now)
	if err != nil {
		log.Printf("online time driver_id=%s period=%s: %v", driverID, period, err)
		writeJSONError(w, http.StatusInternalServerError, "internal_error", "failed to load online time")
		return
	}

	response := bucketOnlineTime(rows, period, since, now, windowDays)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// bucketOnlineTime is the pure aggregation step (mirrors bucketEarnings).
// Unlike a trip's single completed_at timestamp, a session has a
// [started_at, ended_at-or-now) range that can span the query window's
// edges or multiple calendar days, so each row is clipped against the
// overall window and then, for period=week, against each day in turn.
func bucketOnlineTime(rows []offers.OnlineSessionRow, period string, since, now time.Time, windowDays int) onlineTimeResponse {
	response := onlineTimeResponse{Period: period}

	var dailyBuckets []onlineTimeDailyEntry
	if period == periodWeek {
		dailyBuckets = make([]onlineTimeDailyEntry, windowDays)
		for i := range dailyBuckets {
			dailyBuckets[i].Date = since.AddDate(0, 0, i).Format("2006-01-02")
		}
	}

	for _, row := range rows {
		sessionEnd := now
		if row.EndedAt != nil {
			sessionEnd = *row.EndedAt
		}
		overlapStart := maxTime(row.StartedAt, since)
		overlapEnd := minTime(sessionEnd, now)
		if !overlapEnd.After(overlapStart) {
			continue
		}
		response.TotalMinutes += overlapEnd.Sub(overlapStart).Minutes()

		if dailyBuckets == nil {
			continue
		}
		for i := range dailyBuckets {
			dayStart := since.AddDate(0, 0, i)
			dayEnd := dayStart.AddDate(0, 0, 1)
			segStart := maxTime(overlapStart, dayStart)
			segEnd := minTime(overlapEnd, dayEnd)
			if segEnd.After(segStart) {
				dailyBuckets[i].OnlineMinutes += segEnd.Sub(segStart).Minutes()
			}
		}
	}
	response.Daily = dailyBuckets

	return response
}
