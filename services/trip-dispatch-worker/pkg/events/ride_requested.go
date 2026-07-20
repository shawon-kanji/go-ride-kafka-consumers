package events

import "time"

// RideRequestedV1 is the input event contract consumed for dispatch.
// Field subset matches services/cab-request-handler/pkg/events/ride_requested.go;
// fare-breakdown fields are intentionally omitted since the dispatcher doesn't need them.
type RideRequestedV1 struct {
	RequestID      string    `json:"request_id"`
	TripID         string    `json:"trip_id"`
	RiderID        string    `json:"rider_id"`
	Status         string    `json:"status"`
	PickupLat      float64   `json:"pickup_lat"`
	PickupLng      float64   `json:"pickup_lng"`
	DropoffLat     float64   `json:"dropoff_lat"`
	DropoffLng     float64   `json:"dropoff_lng"`
	PickupGeohash  string    `json:"pickup_geohash,omitempty"`
	PickupS2CellID string    `json:"pickup_s2_cell_id,omitempty"`
	SearchRadiusKM float64   `json:"search_radius_km,omitempty"`
	RequestedAt    time.Time `json:"requested_at"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
	EventID        string    `json:"event_id"`
	PublishedAt    time.Time `json:"published_at"`
}
