package events

import "time"

// RideCompletedV1 is published once a driver confirms cash payment was
// collected (awaiting_payment -> completed) — the trip's terminal state.
// FinalFare/CurrencyCode are repeated from RideEndedV1 so this event is
// self-contained for a receipt view.
type RideCompletedV1 struct {
	RequestID          string    `json:"request_id"`
	TripID             string    `json:"trip_id"`
	OngoingTripID      string    `json:"ongoing_trip_id"`
	RiderID            string    `json:"rider_id"`
	DriverID           string    `json:"driver_id"`
	FinalFare          float64   `json:"final_fare"`
	CurrencyCode       string    `json:"currency_code"`
	PaymentCollectedAt time.Time `json:"payment_collected_at"`
	CorrelationID      string    `json:"correlation_id,omitempty"`
	EventID            string    `json:"event_id"`
	PublishedAt        time.Time `json:"published_at"`
}
