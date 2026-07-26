package tracking

import (
	"context"
	"encoding/json"
	"fmt"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/presence"
	"github.com/shawon-kanji/go-ride-utils/events"
)

// Notifier is the filtering chokepoint that keeps this pipeline cheap at
// fleet-wide ping volume: it does exactly one presence.Store lookup per
// incoming location event, and only publishes to Redis (fanning out to
// every gateway instance) when that driver is actually mid-trip. Every
// other ping — the vast majority, from idle/available drivers — is dropped
// here, before any other instance ever sees it.
type Notifier struct {
	store *presence.Store
	bus   *presence.Bus
}

func NewNotifier(store *presence.Store, bus *presence.Bus) *Notifier {
	return &Notifier{store: store, bus: bus}
}

func (n *Notifier) HandleDriverLocationUpdatedEvent(ctx context.Context, event events.DriverLocationUpdatedV1) error {
	trip, ok, err := n.store.GetActiveTrip(ctx, event.DriverID)
	if err != nil {
		return fmt.Errorf("lookup active trip driver_id=%s: %w", event.DriverID, err)
	}
	if !ok {
		return nil
	}

	broadcast := LocationBroadcast{
		RiderID:       trip.RiderID,
		DriverID:      event.DriverID,
		TripID:        trip.TripID,
		OngoingTripID: trip.OngoingTripID,
		Latitude:      event.Latitude,
		Longitude:     event.Longitude,
		AccuracyM:     event.AccuracyM,
		EventTime:     event.EventTime,
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
