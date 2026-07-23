package events

import "time"

// RideAssignedV1 mirrors services/driver-request-handler/pkg/events/ride_assigned.go
// (the producer side). DriverLat/DriverLng are a one-time snapshot taken at
// accept time, not a live feed.
type RideAssignedV1 struct {
	RequestID     string    `json:"request_id"`
	TripID        string    `json:"trip_id"`
	OngoingTripID string    `json:"ongoing_trip_id"`
	RiderID       string    `json:"rider_id"`
	DriverID      string    `json:"driver_id"`
	DriverName    string    `json:"driver_name,omitempty"`
	DriverLat     *float64  `json:"driver_lat,omitempty"`
	DriverLng     *float64  `json:"driver_lng,omitempty"`
	PickupLat     float64   `json:"pickup_lat"`
	PickupLng     float64   `json:"pickup_lng"`
	DropoffLat    float64   `json:"dropoff_lat"`
	DropoffLng    float64   `json:"dropoff_lng"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	EventID       string    `json:"event_id"`
	PublishedAt   time.Time `json:"published_at"`
}
