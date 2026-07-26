package tripend

import "time"

// TripEndedBroadcast is the Redis pub/sub wire format for a trip-ended
// event, mirroring tripstart.TripStartedBroadcast's shape.
type TripEndedBroadcast struct {
	RequestID     string    `json:"request_id"`
	TripID        string    `json:"trip_id"`
	OngoingTripID string    `json:"ongoing_trip_id"`
	RiderID       string    `json:"rider_id"`
	DriverID      string    `json:"driver_id"`
	FinalFare     float64   `json:"final_fare"`
	CurrencyCode  string    `json:"currency_code"`
	EndedAt       time.Time `json:"ended_at"`
}
