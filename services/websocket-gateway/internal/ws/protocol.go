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
	RiderName        string    `json:"rider_name,omitempty"`
	PickupLat        float64   `json:"pickup_lat"`
	PickupLng        float64   `json:"pickup_lng"`
	DropoffLat       float64   `json:"dropoff_lat"`
	DropoffLng       float64   `json:"dropoff_lng"`
	// TripDistanceKM/TripDurationMinutes are the whole trip's real route
	// (pickup to dropoff); zero when the fare had none (haversine fallback
	// at estimate time). PickupDistanceKM/PickupETAMinutes are this
	// specific driver's distance to the pickup point.
	TripDistanceKM      float64   `json:"trip_distance_km,omitempty"`
	TripDurationMinutes float64   `json:"trip_duration_minutes,omitempty"`
	PickupDistanceKM    float64   `json:"pickup_distance_km"`
	PickupETAMinutes    float64   `json:"pickup_eta_minutes"`
	EstimatedEarning    float64   `json:"estimated_earning,omitempty"`
	CurrencyCode        string    `json:"currency_code,omitempty"`
	ExpiresAt           time.Time `json:"expires_at"`
	CorrelationID       string    `json:"correlation_id,omitempty"`
	SentAt              time.Time `json:"sent_at"`
}

// JobOfferParams carries the fields needed to build a JobOfferMessage,
// grouped here so callers (the live-push and replay paths) don't have to
// match a long positional argument list.
type JobOfferParams struct {
	JobOfferID          string
	RequestID           string
	TripID              string
	OfferRank           int
	RiderName           string
	PickupLat           float64
	PickupLng           float64
	DropoffLat          float64
	DropoffLng          float64
	TripDistanceKM      float64
	TripDurationMinutes float64
	PickupDistanceKM    float64
	PickupETAMinutes    float64
	EstimatedEarning    float64
	CurrencyCode        string
	ExpiresAt           time.Time
	CorrelationID       string
}

func NewJobOfferMessage(p JobOfferParams) JobOfferMessage {
	return JobOfferMessage{
		Type:                "job_offer",
		JobOfferID:          p.JobOfferID,
		RequestID:           p.RequestID,
		TripID:              p.TripID,
		OfferRank:           p.OfferRank,
		OfferVersion:        offerVersion,
		RiderName:           p.RiderName,
		PickupLat:           p.PickupLat,
		PickupLng:           p.PickupLng,
		DropoffLat:          p.DropoffLat,
		DropoffLng:          p.DropoffLng,
		TripDistanceKM:      p.TripDistanceKM,
		TripDurationMinutes: p.TripDurationMinutes,
		PickupDistanceKM:    p.PickupDistanceKM,
		PickupETAMinutes:    p.PickupETAMinutes,
		EstimatedEarning:    p.EstimatedEarning,
		CurrencyCode:        p.CurrencyCode,
		ExpiresAt:           p.ExpiresAt,
		CorrelationID:       p.CorrelationID,
		SentAt:              time.Now().UTC(),
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

// RideAssignedMessage is pushed once to the rider once a driver wins the
// accept race. Fire-and-forget — no ack expected back, unlike JobOfferMessage.
type RideAssignedMessage struct {
	Type          string    `json:"type"`
	RequestID     string    `json:"request_id"`
	TripID        string    `json:"trip_id"`
	OngoingTripID string    `json:"ongoing_trip_id"`
	DriverID      string    `json:"driver_id"`
	DriverName    string    `json:"driver_name,omitempty"`
	DriverLat     *float64  `json:"driver_lat,omitempty"`
	DriverLng     *float64  `json:"driver_lng,omitempty"`
	PickupLat     float64   `json:"pickup_lat"`
	PickupLng     float64   `json:"pickup_lng"`
	DropoffLat    float64   `json:"dropoff_lat"`
	DropoffLng    float64   `json:"dropoff_lng"`
	StartPin      string    `json:"start_pin,omitempty"`
	VehicleColor  string    `json:"vehicle_color,omitempty"`
	VehiclePlate  string    `json:"vehicle_plate,omitempty"`
	VehicleModel  string    `json:"vehicle_model,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	SentAt        time.Time `json:"sent_at"`
}

type RideAssignedParams struct {
	RequestID     string
	TripID        string
	OngoingTripID string
	DriverID      string
	DriverName    string
	DriverLat     *float64
	DriverLng     *float64
	PickupLat     float64
	PickupLng     float64
	DropoffLat    float64
	DropoffLng    float64
	StartPin      string
	VehicleColor  string
	VehiclePlate  string
	VehicleModel  string
	CorrelationID string
}

func NewRideAssignedMessage(p RideAssignedParams) RideAssignedMessage {
	return RideAssignedMessage{
		Type:          "ride_assigned",
		RequestID:     p.RequestID,
		TripID:        p.TripID,
		OngoingTripID: p.OngoingTripID,
		DriverID:      p.DriverID,
		DriverName:    p.DriverName,
		DriverLat:     p.DriverLat,
		DriverLng:     p.DriverLng,
		PickupLat:     p.PickupLat,
		PickupLng:     p.PickupLng,
		StartPin:      p.StartPin,
		DropoffLat:    p.DropoffLat,
		DropoffLng:    p.DropoffLng,
		VehicleColor:  p.VehicleColor,
		VehiclePlate:  p.VehiclePlate,
		VehicleModel:  p.VehicleModel,
		CorrelationID: p.CorrelationID,
		SentAt:        time.Now().UTC(),
	}
}

// DriverLocationMessage is pushed to the rider on every location ping from
// their assigned driver, from accept until pickup. Fire-and-forget, same as
// RideAssignedMessage — no ack expected back.
type DriverLocationMessage struct {
	Type          string    `json:"type"`
	TripID        string    `json:"trip_id"`
	OngoingTripID string    `json:"ongoing_trip_id"`
	DriverID      string    `json:"driver_id"`
	Latitude      float64   `json:"latitude"`
	Longitude     float64   `json:"longitude"`
	AccuracyM     float64   `json:"accuracy_m,omitempty"`
	EventTime     time.Time `json:"event_time"`
	// DistanceRemainingKM/EtaMinutes are straight-line-to-pickup, recomputed
	// on every ping — not a re-routed real distance/duration.
	DistanceRemainingKM float64   `json:"distance_remaining_km"`
	EtaMinutes          float64   `json:"eta_minutes"`
	SentAt              time.Time `json:"sent_at"`
}

type DriverLocationParams struct {
	TripID              string
	OngoingTripID       string
	DriverID            string
	Latitude            float64
	Longitude           float64
	AccuracyM           float64
	EventTime           time.Time
	DistanceRemainingKM float64
	EtaMinutes          float64
}

func NewDriverLocationMessage(p DriverLocationParams) DriverLocationMessage {
	return DriverLocationMessage{
		Type:                "driver_location",
		TripID:              p.TripID,
		OngoingTripID:       p.OngoingTripID,
		DriverID:            p.DriverID,
		Latitude:            p.Latitude,
		Longitude:           p.Longitude,
		AccuracyM:           p.AccuracyM,
		EventTime:           p.EventTime,
		DistanceRemainingKM: p.DistanceRemainingKM,
		EtaMinutes:          p.EtaMinutes,
		SentAt:              time.Now().UTC(),
	}
}

// TripStartedMessage is pushed once to the rider when the driver marks the
// trip started (pickup complete, rider onboard). Fire-and-forget, same as
// RideAssignedMessage/DriverLocationMessage — no ack expected back. This is
// also the rider's signal that driver_location pings are about to stop.
type TripStartedMessage struct {
	Type          string    `json:"type"`
	RequestID     string    `json:"request_id"`
	TripID        string    `json:"trip_id"`
	OngoingTripID string    `json:"ongoing_trip_id"`
	DriverID      string    `json:"driver_id"`
	StartedAt     time.Time `json:"started_at"`
	VehicleColor  string    `json:"vehicle_color,omitempty"`
	VehiclePlate  string    `json:"vehicle_plate,omitempty"`
	VehicleModel  string    `json:"vehicle_model,omitempty"`
	SentAt        time.Time `json:"sent_at"`
}

type TripStartedParams struct {
	RequestID     string
	TripID        string
	OngoingTripID string
	DriverID      string
	StartedAt     time.Time
	VehicleColor  string
	VehiclePlate  string
	VehicleModel  string
}

func NewTripStartedMessage(p TripStartedParams) TripStartedMessage {
	return TripStartedMessage{
		Type:          "trip_started",
		RequestID:     p.RequestID,
		TripID:        p.TripID,
		OngoingTripID: p.OngoingTripID,
		DriverID:      p.DriverID,
		StartedAt:     p.StartedAt,
		VehicleColor:  p.VehicleColor,
		VehiclePlate:  p.VehiclePlate,
		VehicleModel:  p.VehicleModel,
		SentAt:        time.Now().UTC(),
	}
}

// TripEndedMessage is pushed once to the rider when the driver marks the
// trip ended at the destination (in_progress -> awaiting_payment). Carries
// the final fare due so the rider can see the amount before handing over
// cash. Fire-and-forget, no ack expected back.
type TripEndedMessage struct {
	Type          string    `json:"type"`
	RequestID     string    `json:"request_id"`
	TripID        string    `json:"trip_id"`
	OngoingTripID string    `json:"ongoing_trip_id"`
	DriverID      string    `json:"driver_id"`
	FinalFare     float64   `json:"final_fare"`
	CurrencyCode  string    `json:"currency_code"`
	EndedAt       time.Time `json:"ended_at"`
	SentAt        time.Time `json:"sent_at"`
}

type TripEndedParams struct {
	RequestID     string
	TripID        string
	OngoingTripID string
	DriverID      string
	FinalFare     float64
	CurrencyCode  string
	EndedAt       time.Time
}

func NewTripEndedMessage(p TripEndedParams) TripEndedMessage {
	return TripEndedMessage{
		Type:          "trip_ended",
		RequestID:     p.RequestID,
		TripID:        p.TripID,
		OngoingTripID: p.OngoingTripID,
		DriverID:      p.DriverID,
		FinalFare:     p.FinalFare,
		CurrencyCode:  p.CurrencyCode,
		EndedAt:       p.EndedAt,
		SentAt:        time.Now().UTC(),
	}
}

// TripCompletedMessage is pushed once to the rider when the driver confirms
// cash payment was collected (awaiting_payment -> completed) — the trip's
// terminal state. Fire-and-forget, no ack expected back.
type TripCompletedMessage struct {
	Type               string    `json:"type"`
	RequestID          string    `json:"request_id"`
	TripID             string    `json:"trip_id"`
	OngoingTripID      string    `json:"ongoing_trip_id"`
	DriverID           string    `json:"driver_id"`
	FinalFare          float64   `json:"final_fare"`
	CurrencyCode       string    `json:"currency_code"`
	PaymentCollectedAt time.Time `json:"payment_collected_at"`
	SentAt             time.Time `json:"sent_at"`
}

type TripCompletedParams struct {
	RequestID          string
	TripID             string
	OngoingTripID      string
	DriverID           string
	FinalFare          float64
	CurrencyCode       string
	PaymentCollectedAt time.Time
}

func NewTripCompletedMessage(p TripCompletedParams) TripCompletedMessage {
	return TripCompletedMessage{
		Type:               "trip_completed",
		RequestID:          p.RequestID,
		TripID:             p.TripID,
		OngoingTripID:      p.OngoingTripID,
		DriverID:           p.DriverID,
		FinalFare:          p.FinalFare,
		CurrencyCode:       p.CurrencyCode,
		PaymentCollectedAt: p.PaymentCollectedAt,
		SentAt:             time.Now().UTC(),
	}
}

// TripCancelledMessage is pushed to both the rider and (if one was assigned)
// the driver when a rider cancels, at whichever stage it happened. Distinct
// from OfferWithdrawnMessage since the two have different audiences and
// different client-side handling (tearing down an active-trip screen vs.
// clearing a pending-offer card).
type TripCancelledMessage struct {
	Type          string    `json:"type"`
	RequestID     string    `json:"request_id"`
	TripID        string    `json:"trip_id"`
	OngoingTripID string    `json:"ongoing_trip_id,omitempty"`
	DriverID      string    `json:"driver_id,omitempty"`
	Stage         string    `json:"stage"`
	CancelledBy   string    `json:"cancelled_by"`
	CancelledAt   time.Time `json:"cancelled_at"`
	SentAt        time.Time `json:"sent_at"`
}

type TripCancelledParams struct {
	RequestID     string
	TripID        string
	OngoingTripID string
	DriverID      string
	Stage         string
	CancelledBy   string
	CancelledAt   time.Time
}

func NewTripCancelledMessage(p TripCancelledParams) TripCancelledMessage {
	return TripCancelledMessage{
		Type:          "trip_cancelled",
		RequestID:     p.RequestID,
		TripID:        p.TripID,
		OngoingTripID: p.OngoingTripID,
		DriverID:      p.DriverID,
		Stage:         p.Stage,
		CancelledBy:   p.CancelledBy,
		CancelledAt:   p.CancelledAt,
		SentAt:        time.Now().UTC(),
	}
}

// OfferWithdrawnMessage is pushed to a driver whose still-pending job offer
// was withdrawn because the rider cancelled before anyone accepted.
type OfferWithdrawnMessage struct {
	Type      string    `json:"type"`
	RequestID string    `json:"request_id"`
	TripID    string    `json:"trip_id"`
	SentAt    time.Time `json:"sent_at"`
}

type OfferWithdrawnParams struct {
	RequestID string
	TripID    string
}

func NewOfferWithdrawnMessage(p OfferWithdrawnParams) OfferWithdrawnMessage {
	return OfferWithdrawnMessage{
		Type:      "offer_withdrawn",
		RequestID: p.RequestID,
		TripID:    p.TripID,
		SentAt:    time.Now().UTC(),
	}
}
