package api

import (
	"net/http"
	"time"
)

// periodToday/periodWeek are the two windows both /earnings and
// /online-time accept. weekWindowDays is a trailing 7-day window (today
// plus the six days before it), not a calendar week — simpler and avoids a
// partial first bar right after a week/month boundary. Day boundaries
// throughout are UTC calendar days: this system stores every timestamp in
// UTC and has no per-driver timezone anywhere in the schema, so a UTC-day
// bucket is the simplest option that's still consistent with the rest of
// the backend; it will misalign with a driver's actual local day by their
// UTC offset, a known simplification.
const (
	periodToday    = "today"
	periodWeek     = "week"
	weekWindowDays = 7
)

// parsePeriodWindow validates the ?period= query param and returns the
// window's start (UTC midnight, weekWindowDays-1 days back for "week") and
// its day-count. Returns ok=false (having already written the error
// response) on an invalid period.
func parsePeriodWindow(w http.ResponseWriter, r *http.Request, now time.Time) (period string, since time.Time, windowDays int, ok bool) {
	period = r.URL.Query().Get("period")
	if period == "" {
		period = periodToday
	}
	if period != periodToday && period != periodWeek {
		writeJSONError(w, http.StatusBadRequest, "invalid_period", "period must be one of: today, week")
		return "", time.Time{}, 0, false
	}

	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	windowDays = 1
	if period == periodWeek {
		windowDays = weekWindowDays
	}
	since = today.AddDate(0, 0, -(windowDays - 1))
	return period, since, windowDays, true
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
