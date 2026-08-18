package offers

import "testing"

func TestDerefReturnsZeroValueForNil(t *testing.T) {
	var p *float64
	if got := deref(p); got != 0 {
		t.Fatalf("deref(nil) = %v, want 0", got)
	}

	v := 4.5
	if got := deref(&v); got != v {
		t.Fatalf("deref(&v) = %v, want %v", got, v)
	}
}

func TestHaversineKMKnownDistance(t *testing.T) {
	// Singapore CBD (Raffles Place) to Changi Airport, ~18km straight-line.
	got := haversineKM(1.2839, 103.8517, 1.3644, 103.9915)
	if got < 16 || got > 20 {
		t.Fatalf("haversineKM = %v, want roughly 16-20km", got)
	}
}
