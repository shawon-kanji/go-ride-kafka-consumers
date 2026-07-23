package events

import "time"

// JobOfferV1 is the event published by trip-dispatch-worker once per dispatch
// attempt that produces offers. Field subset mirrors
// services/trip-dispatch-worker/pkg/events/job_offer.go (the producer side).
type JobOfferV1 struct {
	RequestID        string          `json:"request_id"`
	TripID           string          `json:"trip_id"`
	RiderID          string          `json:"rider_id"`
	PickupLat        float64         `json:"pickup_lat"`
	PickupLng        float64         `json:"pickup_lng"`
	DropoffLat       float64         `json:"dropoff_lat"`
	DropoffLng       float64         `json:"dropoff_lng"`
	Offers           []JobOfferEntry `json:"offers"`
	EstimatedEarning float64         `json:"estimated_earning,omitempty"`
	CurrencyCode     string          `json:"currency_code,omitempty"`
	CorrelationID    string          `json:"correlation_id,omitempty"`
	EventID          string          `json:"event_id"`
	PublishedAt      time.Time       `json:"published_at"`
}

// JobOfferEntry is one driver's offer within a JobOfferV1 batch.
type JobOfferEntry struct {
	JobOfferID string    `json:"job_offer_id"`
	DriverID   string    `json:"driver_id"`
	OfferRank  int       `json:"offer_rank"`
	OfferedAt  time.Time `json:"offered_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}
