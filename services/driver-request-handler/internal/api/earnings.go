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

const (
	earningsPeriodToday = "today"
	earningsPeriodWeek  = "week"

	// earningsWeekDays is a trailing 7-day window (today plus the six days
	// before it), not a calendar week — simpler and avoids a partial first
	// bar right after a week/month boundary. Matches D11's "seven-bar
	// per-day chart".
	earningsWeekDays = 7
)

type earningsDailyEntry struct {
	Date      string  `json:"date"` // YYYY-MM-DD, UTC calendar day
	Earnings  float64 `json:"earnings"`
	TripCount int     `json:"trip_count"`
}

type earningsResponse struct {
	Period        string               `json:"period"`
	CurrencyCode  string               `json:"currency_code,omitempty"`
	TotalEarnings float64              `json:"total_earnings"`
	TripCount     int                  `json:"trip_count"`
	Daily         []earningsDailyEntry `json:"daily,omitempty"`
}

// handleEarnings backs D06's "today's earnings" stat and D11's week total +
// trip count + seven-bar chart. Day boundaries are UTC calendar days — this
// system stores every timestamp in UTC and has no per-driver timezone
// anywhere in the schema, so a UTC-day bucket is the simplest option that's
// still consistent with the rest of the backend; it will misalign with the
// driver's actual local day by their UTC offset, a known simplification.
func (s *Server) handleEarnings(w http.ResponseWriter, r *http.Request) {
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

	period := r.URL.Query().Get("period")
	if period == "" {
		period = earningsPeriodToday
	}
	if period != earningsPeriodToday && period != earningsPeriodWeek {
		writeJSONError(w, http.StatusBadRequest, "invalid_period", "period must be one of: today, week")
		return
	}

	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	windowDays := 1
	if period == earningsPeriodWeek {
		windowDays = earningsWeekDays
	}
	since := today.AddDate(0, 0, -(windowDays - 1))

	rows, err := s.offers.EarningsSince(r.Context(), driverID, since)
	if err != nil {
		log.Printf("earnings driver_id=%s period=%s: %v", driverID, period, err)
		writeJSONError(w, http.StatusInternalServerError, "internal_error", "failed to load earnings")
		return
	}

	response := bucketEarnings(rows, period, since, windowDays)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// bucketEarnings is the pure aggregation step, split out from the handler so
// the day-bucketing math (the one genuinely fiddly part here) is unit-
// testable without a database. daily buckets are only populated for
// period == earningsPeriodWeek — D06's "today" card just needs the totals.
func bucketEarnings(rows []offers.EarningsRow, period string, since time.Time, windowDays int) earningsResponse {
	response := earningsResponse{Period: period}

	var dailyBuckets []earningsDailyEntry
	if period == earningsPeriodWeek {
		dailyBuckets = make([]earningsDailyEntry, windowDays)
		for i := range dailyBuckets {
			dailyBuckets[i].Date = since.AddDate(0, 0, i).Format("2006-01-02")
		}
	}

	for _, row := range rows {
		response.TotalEarnings += row.FinalFare
		response.TripCount++
		if response.CurrencyCode == "" && row.CurrencyCode != nil {
			response.CurrencyCode = *row.CurrencyCode
		}

		if dailyBuckets == nil {
			continue
		}
		dayIndex := int(row.CompletedAt.UTC().Truncate(24*time.Hour).Sub(since).Hours() / 24)
		if dayIndex < 0 || dayIndex >= len(dailyBuckets) {
			continue
		}
		dailyBuckets[dayIndex].Earnings += row.FinalFare
		dailyBuckets[dayIndex].TripCount++
	}
	response.Daily = dailyBuckets

	return response
}
