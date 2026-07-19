package events

import "time"

// RideRequestedV1 is the event contract for a new cab request.
type RideRequestedV1 struct {
	RequestID       string     `json:"request_id"`
	TripID          string     `json:"trip_id"`
	RiderID         string     `json:"rider_id"`
	FareID          *string    `json:"fare_id,omitempty"`
	Status          string     `json:"status"`
	PickupLat       float64    `json:"pickup_lat"`
	PickupLng       float64    `json:"pickup_lng"`
	DropoffLat      float64    `json:"dropoff_lat"`
	DropoffLng      float64    `json:"dropoff_lng"`
	PickupGeohash   string     `json:"pickup_geohash,omitempty"`
	PickupS2CellID  string     `json:"pickup_s2_cell_id,omitempty"`
	SearchRadiusKM  float64    `json:"search_radius_km,omitempty"`
	RequestedAt     time.Time  `json:"requested_at"`
	CorrelationID   string     `json:"correlation_id,omitempty"`
	EventID         string     `json:"event_id"`
	PublishedAt     time.Time  `json:"published_at"`
	CurrencyCode    string     `json:"currency_code,omitempty"`
	BaseFare        float64    `json:"base_fare,omitempty"`
	DistanceFare    float64    `json:"distance_fare,omitempty"`
	TimeFare        float64    `json:"time_fare,omitempty"`
	SurchargeTotal  float64    `json:"surcharge_total,omitempty"`
	DiscountTotal   float64    `json:"discount_total,omitempty"`
	SurgeMultiplier float64    `json:"surge_multiplier,omitempty"`
	TotalFare       float64    `json:"total_fare,omitempty"`
	PricingVersion  string     `json:"pricing_version,omitempty"`
	FareLockedAt    *time.Time `json:"fare_locked_at,omitempty"`
	FareExpiresAt   *time.Time `json:"fare_expires_at,omitempty"`
}
