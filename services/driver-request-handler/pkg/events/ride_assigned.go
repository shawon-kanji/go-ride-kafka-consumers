package events

import "time"

// RideAssignedV1 is the event contract published once a driver accepts a job
// offer and wins the first-wins assignment lock. DriverLat/DriverLng are a
// one-time snapshot taken from driver_locations at accept time, not a live
// feed — continuous location updates during a trip are a later phase.
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
	VehicleColor  string    `json:"vehicle_color,omitempty"`
	VehiclePlate  string    `json:"vehicle_plate,omitempty"`
	VehicleModel  string    `json:"vehicle_model,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	EventID       string    `json:"event_id"`
	PublishedAt   time.Time `json:"published_at"`
}
