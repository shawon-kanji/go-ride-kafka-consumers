package events

import "time"

// RideStartedV1 is the event contract published once a driver marks a trip
// started (pickup complete, rider onboard). No lat/lng — nothing new
// geographically happens at start, the rider already has pickup location
// from ride.assigned.v1.
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
