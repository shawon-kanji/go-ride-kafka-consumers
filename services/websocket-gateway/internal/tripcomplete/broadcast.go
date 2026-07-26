package tripcomplete

import "time"

// TripCompletedBroadcast is the Redis pub/sub wire format for a
// trip-completed (payment collected) event, mirroring
// tripend.TripEndedBroadcast's shape.
type TripCompletedBroadcast struct {
	RequestID          string    `json:"request_id"`
	TripID             string    `json:"trip_id"`
	OngoingTripID      string    `json:"ongoing_trip_id"`
	RiderID            string    `json:"rider_id"`
	DriverID           string    `json:"driver_id"`
	FinalFare          float64   `json:"final_fare"`
	CurrencyCode       string    `json:"currency_code"`
	PaymentCollectedAt time.Time `json:"payment_collected_at"`
}
