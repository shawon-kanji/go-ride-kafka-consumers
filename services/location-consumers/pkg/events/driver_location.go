package events

import "time"

// DriverLocationUpdatedV1 is the base event contract for location updates.
type DriverLocationUpdatedV1 struct {
	DriverID    string    `json:"driver_id"`
	Latitude    float64   `json:"latitude"`
	Longitude   float64   `json:"longitude"`
	EventTime   time.Time `json:"event_time"`
	Geohash     string    `json:"geohash,omitempty"`
	S2CellID    string    `json:"s2_cell_id,omitempty"`
	AccuracyM   float64   `json:"accuracy_m,omitempty"`
	Source      string    `json:"source,omitempty"`
	EventID     string    `json:"event_id,omitempty"`
	PublishedAt time.Time `json:"published_at,omitempty"`
}
