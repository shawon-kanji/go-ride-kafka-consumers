package events

import "time"

// RideEndedV1 mirrors services/driver-request-handler/pkg/events/ride_ended.go
// (the producer side).
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
