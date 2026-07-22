package ws

import "time"

// offerVersion is a placeholder until driver_job_offers gains a real
// offer_version column for re-offer/reassignment semantics (not needed yet).
const offerVersion = 1

// JobOfferMessage is pushed to a driver over the websocket, one per offer.
type JobOfferMessage struct {
	Type             string    `json:"type"`
	JobOfferID       string    `json:"job_offer_id"`
	RequestID        string    `json:"request_id"`
	TripID           string    `json:"trip_id"`
	OfferRank        int       `json:"offer_rank"`
	OfferVersion     int       `json:"offer_version"`
	PickupLat        float64   `json:"pickup_lat"`
	PickupLng        float64   `json:"pickup_lng"`
	DropoffLat       float64   `json:"dropoff_lat"`
	DropoffLng       float64   `json:"dropoff_lng"`
	EstimatedEarning float64   `json:"estimated_earning,omitempty"`
	CurrencyCode     string    `json:"currency_code,omitempty"`
	ExpiresAt        time.Time `json:"expires_at"`
	CorrelationID    string    `json:"correlation_id,omitempty"`
	SentAt           time.Time `json:"sent_at"`
}

// JobOfferParams carries the fields needed to build a JobOfferMessage,
// grouped here so callers (the live-push and replay paths) don't have to
// match a long positional argument list.
type JobOfferParams struct {
	JobOfferID       string
	RequestID        string
	TripID           string
	OfferRank        int
	PickupLat        float64
	PickupLng        float64
	DropoffLat       float64
	DropoffLng       float64
	EstimatedEarning float64
	CurrencyCode     string
	ExpiresAt        time.Time
	CorrelationID    string
}

func NewJobOfferMessage(p JobOfferParams) JobOfferMessage {
	return JobOfferMessage{
		Type:             "job_offer",
		JobOfferID:       p.JobOfferID,
		RequestID:        p.RequestID,
		TripID:           p.TripID,
		OfferRank:        p.OfferRank,
		OfferVersion:     offerVersion,
		PickupLat:        p.PickupLat,
		PickupLng:        p.PickupLng,
		DropoffLat:       p.DropoffLat,
		DropoffLng:       p.DropoffLng,
		EstimatedEarning: p.EstimatedEarning,
		CurrencyCode:     p.CurrencyCode,
		ExpiresAt:        p.ExpiresAt,
		CorrelationID:    p.CorrelationID,
		SentAt:           time.Now().UTC(),
	}
}

// AckMessage is sent by a driver client to acknowledge a job offer.
// "accepted"/"rejected" are parsed here (so Phase 7 needs no wire-format
// change) but Phase 6 only records delivery bookkeeping for them — no
// assignment side effects, no first-wins locking, no ride.assigned.v1 publish.
type AckMessage struct {
	Type       string `json:"type"`
	JobOfferID string `json:"job_offer_id"`
	Status     string `json:"status"`
}

const (
	AckStatusDelivered = "delivered"
	AckStatusSeen      = "seen"
	AckStatusAccepted  = "accepted"
	AckStatusRejected  = "rejected"
)
