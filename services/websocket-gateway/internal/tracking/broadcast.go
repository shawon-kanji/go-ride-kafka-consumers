package tracking

import "time"

// LocationBroadcast is the Redis pub/sub wire format for a driver location
// ping that passed the active-trip filter. It's deliberately distinct from
// events.DriverLocationUpdatedV1 (which carries no rider_id) — Notifier
// enriches the raw Kafka event with the presence.Store lookup result before
// publishing this.
type LocationBroadcast struct {
	RiderID       string    `json:"rider_id"`
	DriverID      string    `json:"driver_id"`
	TripID        string    `json:"trip_id"`
	OngoingTripID string    `json:"ongoing_trip_id"`
	Latitude      float64   `json:"latitude"`
	Longitude     float64   `json:"longitude"`
	AccuracyM     float64   `json:"accuracy_m,omitempty"`
	EventTime     time.Time `json:"event_time"`
}
