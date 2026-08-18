package api

import (
	"testing"
	"time"

	"go-ride-kafka-consumers/services/driver-request-handler/internal/offers"
)

func mustParseHour(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse("2006-01-02T15:04", s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return tm
}

func TestBucketOnlineTimeTodaySingleClosedSession(t *testing.T) {
	since := mustParseHour(t, "2026-08-18T00:00")
	now := mustParseHour(t, "2026-08-18T18:00")
	started := mustParseHour(t, "2026-08-18T09:00")
	ended := mustParseHour(t, "2026-08-18T11:30")

	rows := []offers.OnlineSessionRow{{StartedAt: started, EndedAt: &ended}}

	got := bucketOnlineTime(rows, periodToday, since, now, 1)

	if got.TotalMinutes != 150 {
		t.Errorf("total_minutes = %v, want 150 (2h30m)", got.TotalMinutes)
	}
	if got.Daily != nil {
		t.Errorf("daily = %v, want nil for period=today", got.Daily)
	}
}

func TestBucketOnlineTimeStillOpenSessionClipsAtNow(t *testing.T) {
	since := mustParseHour(t, "2026-08-18T00:00")
	now := mustParseHour(t, "2026-08-18T14:00")
	started := mustParseHour(t, "2026-08-18T13:00")

	rows := []offers.OnlineSessionRow{{StartedAt: started, EndedAt: nil}}

	got := bucketOnlineTime(rows, periodToday, since, now, 1)

	if got.TotalMinutes != 60 {
		t.Errorf("total_minutes = %v, want 60 (still-open session clipped at now)", got.TotalMinutes)
	}
}

func TestBucketOnlineTimeSessionStartedBeforeWindowClipsAtSince(t *testing.T) {
	since := mustParseHour(t, "2026-08-18T00:00")
	now := mustParseHour(t, "2026-08-18T10:00")
	// Started yesterday evening, still open -- only the portion inside
	// today's window should count.
	started := mustParseHour(t, "2026-08-17T22:00")

	rows := []offers.OnlineSessionRow{{StartedAt: started, EndedAt: nil}}

	got := bucketOnlineTime(rows, periodToday, since, now, 1)

	if got.TotalMinutes != 600 { // 00:00 -> 10:00 = 10h = 600min
		t.Errorf("total_minutes = %v, want 600 (clipped at window start, not session start)", got.TotalMinutes)
	}
}

func TestBucketOnlineTimeWeekSplitsSessionAcrossDays(t *testing.T) {
	since := mustParseHour(t, "2026-08-12T00:00")
	now := mustParseHour(t, "2026-08-18T12:00")
	// A session spanning midnight between day 0 and day 1 of the window.
	started := mustParseHour(t, "2026-08-12T22:00")
	ended := mustParseHour(t, "2026-08-13T02:00")

	rows := []offers.OnlineSessionRow{{StartedAt: started, EndedAt: &ended}}

	got := bucketOnlineTime(rows, periodWeek, since, now, weekWindowDays)

	if len(got.Daily) != 7 {
		t.Fatalf("daily buckets = %d, want 7", len(got.Daily))
	}
	if got.Daily[0].OnlineMinutes != 120 { // 22:00->24:00 = 2h
		t.Errorf("day 0 online_minutes = %v, want 120", got.Daily[0].OnlineMinutes)
	}
	if got.Daily[1].OnlineMinutes != 120 { // 00:00->02:00 = 2h
		t.Errorf("day 1 online_minutes = %v, want 120", got.Daily[1].OnlineMinutes)
	}
	if got.TotalMinutes != 240 {
		t.Errorf("total_minutes = %v, want 240", got.TotalMinutes)
	}
}

func TestBucketOnlineTimeNoSessionsReturnsZero(t *testing.T) {
	since := mustParseHour(t, "2026-08-18T00:00")
	now := mustParseHour(t, "2026-08-18T18:00")

	got := bucketOnlineTime(nil, periodToday, since, now, 1)

	if got.TotalMinutes != 0 {
		t.Errorf("total_minutes = %v, want 0", got.TotalMinutes)
	}
}
