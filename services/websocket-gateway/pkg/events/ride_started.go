package events

import "time"

// RideStartedV1 mirrors services/driver-request-handler/pkg/events/ride_started.go
// (the producer side).
type RideStartedV1 struct {
	RequestID     string    `json:"request_id"`
	TripID        string    `json:"trip_id"`
	OngoingTripID string    `json:"ongoing_trip_id"`
	RiderID       string    `json:"rider_id"`
	DriverID      string    `json:"driver_id"`
	StartedAt     time.Time `json:"started_at"`
	VehicleColor  string    `json:"vehicle_color,omitempty"`
	VehiclePlate  string    `json:"vehicle_plate,omitempty"`
	VehicleModel  string    `json:"vehicle_model,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	EventID       string    `json:"event_id"`
	PublishedAt   time.Time `json:"published_at"`
}
