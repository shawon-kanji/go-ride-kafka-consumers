package tripcancel

import "time"

// TripCancelledBroadcast is the Redis pub/sub wire format for a
// ride-cancelled event, mirroring tripstart.TripStartedBroadcast's shape.
type TripCancelledBroadcast struct {
	RequestID               string    `json:"request_id"`
	TripID                  string    `json:"trip_id"`
	OngoingTripID           string    `json:"ongoing_trip_id,omitempty"`
	RiderID                 string    `json:"rider_id"`
	DriverID                string    `json:"driver_id,omitempty"`
	Stage                   string    `json:"stage"`
	WithdrawnOfferDriverIDs []string  `json:"withdrawn_offer_driver_ids,omitempty"`
	CancelledBy             string    `json:"cancelled_by"`
	CancelledAt             time.Time `json:"cancelled_at"`
}
