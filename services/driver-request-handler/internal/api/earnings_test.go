package api

import (
	"testing"
	"time"

	"go-ride-kafka-consumers/services/driver-request-handler/internal/offers"
)

func mustParseDay(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse("2006-01-02", s)
	if err != nil {
		t.Fatalf("parse day %q: %v", s, err)
	}
	return tm
}

func TestBucketEarningsToday(t *testing.T) {
	today := mustParseDay(t, "2026-08-18")
	sgd := "SGD"
	rows := []offers.EarningsRow{
		{FinalFare: 12.50, CurrencyCode: &sgd, CompletedAt: today.Add(9 * time.Hour)},
		{FinalFare: 7.25, CurrencyCode: &sgd, CompletedAt: today.Add(14 * time.Hour)},
	}

	got := bucketEarnings(rows, earningsPeriodToday, today, 1)

	if got.Period != earningsPeriodToday {
		t.Errorf("period = %q, want %q", got.Period, earningsPeriodToday)
	}
	if got.TotalEarnings != 19.75 {
		t.Errorf("total_earnings = %v, want 19.75", got.TotalEarnings)
	}
	if got.TripCount != 2 {
		t.Errorf("trip_count = %d, want 2", got.TripCount)
	}
	if got.CurrencyCode != "SGD" {
		t.Errorf("currency_code = %q, want SGD", got.CurrencyCode)
	}
	if got.Daily != nil {
		t.Errorf("daily = %v, want nil for period=today", got.Daily)
	}
}

func TestBucketEarningsWeekProducesSevenBucketsOldestFirst(t *testing.T) {
	since := mustParseDay(t, "2026-08-12") // 7-day window: 08-12 .. 08-18
	sgd := "SGD"
	rows := []offers.EarningsRow{
		{FinalFare: 10, CurrencyCode: &sgd, CompletedAt: since.Add(10 * time.Hour)},                 // day 0 (08-12)
		{FinalFare: 5, CurrencyCode: &sgd, CompletedAt: since.AddDate(0, 0, 6).Add(23 * time.Hour)}, // day 6 (08-18), late in the day
		{FinalFare: 3, CurrencyCode: &sgd, CompletedAt: since.AddDate(0, 0, 6).Add(1 * time.Hour)},  // also day 6
	}

	got := bucketEarnings(rows, earningsPeriodWeek, since, 7)

	if len(got.Daily) != 7 {
		t.Fatalf("daily buckets = %d, want 7", len(got.Daily))
	}
	if got.Daily[0].Date != "2026-08-12" || got.Daily[6].Date != "2026-08-18" {
		t.Errorf("daily dates = [%s..%s], want [2026-08-12..2026-08-18]", got.Daily[0].Date, got.Daily[6].Date)
	}
	if got.Daily[0].Earnings != 10 || got.Daily[0].TripCount != 1 {
		t.Errorf("day 0 = %+v, want earnings=10 trip_count=1", got.Daily[0])
	}
	// Middle days with no trips must still be present as zero-value bars,
	// not skipped -- the client renders a fixed 7-bar chart.
	if got.Daily[3].Earnings != 0 || got.Daily[3].TripCount != 0 {
		t.Errorf("day 3 = %+v, want zero", got.Daily[3])
	}
	if got.Daily[6].Earnings != 8 || got.Daily[6].TripCount != 2 {
		t.Errorf("day 6 = %+v, want earnings=8 trip_count=2 (both trips same day)", got.Daily[6])
	}
	if got.TotalEarnings != 18 || got.TripCount != 3 {
		t.Errorf("totals = earnings=%v count=%d, want earnings=18 count=3", got.TotalEarnings, got.TripCount)
	}
}

func TestBucketEarningsNoTripsReturnsZeroedResponse(t *testing.T) {
	today := mustParseDay(t, "2026-08-18")

	got := bucketEarnings(nil, earningsPeriodToday, today, 1)

	if got.TotalEarnings != 0 || got.TripCount != 0 || got.CurrencyCode != "" {
		t.Errorf("got %+v, want all-zero response", got)
	}
}
