package tripstart

import "time"

// TripStartedBroadcast is the Redis pub/sub wire format for a trip-started
// event, mirroring tracking.LocationBroadcast's shape.
type TripStartedBroadcast struct {
	RequestID     string    `json:"request_id"`
	TripID        string    `json:"trip_id"`
	OngoingTripID string    `json:"ongoing_trip_id"`
	RiderID       string    `json:"rider_id"`
	DriverID      string    `json:"driver_id"`
	StartedAt     time.Time `json:"started_at"`
	VehicleColor  string    `json:"vehicle_color,omitempty"`
	VehiclePlate  string    `json:"vehicle_plate,omitempty"`
	VehicleModel  string    `json:"vehicle_model,omitempty"`
}
