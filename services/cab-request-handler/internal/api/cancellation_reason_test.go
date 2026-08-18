package api

import "testing"

func TestIsValidCancellationReason(t *testing.T) {
	valid := []string{"rider_absent", "rider_requested", "vehicle_problem", "unsafe_destination", "other"}
	for _, reason := range valid {
		if !isValidCancellationReason(reason) {
			t.Errorf("expected %q to be valid", reason)
		}
	}

	invalid := []string{"", "RIDER_ABSENT", "traffic jam", "rider absent", "unknown"}
	for _, reason := range invalid {
		if isValidCancellationReason(reason) {
			t.Errorf("expected %q to be invalid", reason)
		}
	}
}
