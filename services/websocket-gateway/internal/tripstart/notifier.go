package tripstart

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/presence"
	"github.com/shawon-kanji/go-ride-utils/events"
)

// Notifier reacts to a RideStartedV1 event two ways: it stops the location
// stream (presence.Store.ClearActiveTrip) and broadcasts a trip-started
// notice to Redis so every gateway instance can push it to the rider over
// its local hub — same broadcast-and-filter design as the other pipelines.
type Notifier struct {
	store *presence.Store
	bus   *presence.Bus
}

func NewNotifier(store *presence.Store, bus *presence.Bus) *Notifier {
	return &Notifier{store: store, bus: bus}
}

func (n *Notifier) HandleRideStartedEvent(ctx context.Context, event events.RideStartedV1) error {
	// Best-effort: a Redis hiccup here must not block telling the rider the
	// trip started — the mapping self-heals via TTL either way.
	if err := n.store.ClearActiveTrip(ctx, event.DriverID); err != nil {
		log.Printf("websocket_gateway: clear active trip failed driver_id=%s request_id=%s: %v", event.DriverID, event.RequestID, err)
	}

	broadcast := TripStartedBroadcast{
		RequestID:     event.RequestID,
		TripID:        event.TripID,
		OngoingTripID: event.OngoingTripID,
		RiderID:       event.RiderID,
		DriverID:      event.DriverID,
		StartedAt:     event.StartedAt,
		VehicleColor:  event.VehicleColor,
		VehiclePlate:  event.VehiclePlate,
		VehicleModel:  event.VehicleModel,
	}

	payload, err := json.Marshal(broadcast)
	if err != nil {
		return fmt.Errorf("marshal trip started broadcast request_id=%s: %w", event.RequestID, err)
	}

	if err := n.bus.Publish(ctx, payload); err != nil {
		return fmt.Errorf("publish trip started broadcast request_id=%s: %w", event.RequestID, err)
	}
	return nil
}
