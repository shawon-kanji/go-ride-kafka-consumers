package events

import "time"

// RideRequestedV1 is the input event contract for dispatch processing.
type RideRequestedV1 struct {
	RideID        string    `json:"ride_id"`
	RiderID       string    `json:"rider_id"`
	PickupLat     float64   `json:"pickup_lat"`
	PickupLng     float64   `json:"pickup_lng"`
	DropoffLat    float64   `json:"dropoff_lat"`
	DropoffLng    float64   `json:"dropoff_lng"`
	RequestedAt   time.Time `json:"requested_at"`
	CorrelationID string    `json:"correlation_id,omitempty"`
}
