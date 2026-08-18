package tracking

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/shawon-kanji/go-ride-utils/events"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/presence"
)

// Notifier is the filtering chokepoint that keeps this pipeline cheap at
// fleet-wide ping volume: it does exactly one presence.Store lookup per
// incoming location event, and only publishes to Redis (fanning out to
// every gateway instance) when that driver is actually mid-trip. Every
// other ping — the vast majority, from idle/available drivers — is dropped
// here, before any other instance ever sees it.
type Notifier struct {
	store               *presence.Store
	bus                 *presence.Bus
	fallbackAvgSpeedKPH float64
}

func NewNotifier(store *presence.Store, bus *presence.Bus, fallbackAvgSpeedKPH float64) *Notifier {
	return &Notifier{store: store, bus: bus, fallbackAvgSpeedKPH: fallbackAvgSpeedKPH}
}

func (n *Notifier) HandleDriverLocationUpdatedEvent(ctx context.Context, event events.DriverLocationUpdatedV1) error {
	trip, ok, err := n.store.GetActiveTrip(ctx, event.DriverID)
	if err != nil {
		return fmt.Errorf("lookup active trip driver_id=%s: %w", event.DriverID, err)
	}
	if !ok {
		return nil
	}

	distanceRemainingKM := haversineKM(event.Latitude, event.Longitude, trip.PickupLat, trip.PickupLng)
	avgSpeedKPH := n.fallbackAvgSpeedKPH
	if trip.AvgSpeedKPH != nil && *trip.AvgSpeedKPH > 0 {
		avgSpeedKPH = *trip.AvgSpeedKPH
	}

	broadcast := LocationBroadcast{
		RiderID:             trip.RiderID,
		DriverID:            event.DriverID,
		TripID:              trip.TripID,
		OngoingTripID:       trip.OngoingTripID,
		Latitude:            event.Latitude,
		Longitude:           event.Longitude,
		AccuracyM:           event.AccuracyM,
		EventTime:           event.EventTime,
		DistanceRemainingKM: distanceRemainingKM,
		EtaMinutes:          (distanceRemainingKM / avgSpeedKPH) * 60,
	}

	payload, err := json.Marshal(broadcast)
	if err != nil {
		return fmt.Errorf("marshal location broadcast driver_id=%s: %w", event.DriverID, err)
	}

	if err := n.bus.Publish(ctx, payload); err != nil {
		return fmt.Errorf("publish location broadcast driver_id=%s: %w", event.DriverID, err)
	}
	return nil
}

// haversineKM is deliberately duplicated here rather than shared — same
// small-helper-per-service convention cab-request-handler's own haversineKM
// already follows in this codebase.
func haversineKM(lat1, lng1, lat2, lng2 float64) float64 {
	const earthRadiusKM = 6371.0

	lat1Rad := lat1 * math.Pi / 180
	lng1Rad := lng1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180
	lng2Rad := lng2 * math.Pi / 180

	deltaLat := lat2Rad - lat1Rad
	deltaLng := lng2Rad - lng1Rad

	sinLat := math.Sin(deltaLat / 2)
	sinLng := math.Sin(deltaLng / 2)
	a := sinLat*sinLat + math.Cos(lat1Rad)*math.Cos(lat2Rad)*sinLng*sinLng
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadiusKM * c
}
