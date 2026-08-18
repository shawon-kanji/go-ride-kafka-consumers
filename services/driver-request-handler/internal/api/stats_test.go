package api

import "testing"

func TestComputeAcceptanceScoreNoOffersReturnsNil(t *testing.T) {
	if got := computeAcceptanceScore(0, 0, 0); got != nil {
		t.Errorf("computeAcceptanceScore(0,0,0) = %v, want nil", got)
	}
}

func TestComputeAcceptanceScorePerfectRecord(t *testing.T) {
	got := computeAcceptanceScore(10, 0, 10)
	if got == nil || *got != 100 {
		t.Fatalf("got %v, want 100", got)
	}
}

func TestComputeAcceptanceScoreCancellationsReduceScore(t *testing.T) {
	// 10 offers, 8 accepted, 3 of those later cancelled by the driver ->
	// (8-3)/10 = 50%.
	got := computeAcceptanceScore(8, 3, 10)
	if got == nil || *got != 50 {
		t.Fatalf("got %v, want 50", got)
	}
}

func TestComputeAcceptanceScoreClampsAtZeroNotNegative(t *testing.T) {
	// More cancellations than accepted offers shouldn't be possible in
	// practice, but the formula must still clamp rather than go negative.
	got := computeAcceptanceScore(2, 5, 10)
	if got == nil || *got != 0 {
		t.Fatalf("got %v, want 0 (clamped)", got)
	}
}

func TestComputeAcceptanceScoreClampsAtHundred(t *testing.T) {
	got := computeAcceptanceScore(10, 0, 5) // pathological but must not exceed 100
	if got == nil || *got != 100 {
		t.Fatalf("got %v, want 100 (clamped)", got)
	}
}
