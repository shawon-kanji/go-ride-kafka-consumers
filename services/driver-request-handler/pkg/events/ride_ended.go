package events

import "time"

// RideEndedV1 is published once a driver marks a trip ended at the
// destination (in_progress -> awaiting_payment). FinalFare/CurrencyCode are
// the already-locked trip_fares.total_fare/currency_code as-is — no live
// recalculation, same trust-the-driver's-tap model as accept/start-trip.
type RideEndedV1 struct {
	RequestID     string    `json:"request_id"`
	TripID        string    `json:"trip_id"`
	OngoingTripID string    `json:"ongoing_trip_id"`
	RiderID       string    `json:"rider_id"`
	DriverID      string    `json:"driver_id"`
	FinalFare     float64   `json:"final_fare"`
	CurrencyCode  string    `json:"currency_code"`
	EndedAt       time.Time `json:"ended_at"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	EventID       string    `json:"event_id"`
	PublishedAt   time.Time `json:"published_at"`
}
