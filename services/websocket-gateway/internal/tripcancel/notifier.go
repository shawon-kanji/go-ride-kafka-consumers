package tripcancel

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/presence"

	"github.com/shawon-kanji/go-ride-utils/events"
)

// Notifier reacts to a RideCancelledV1 event: if a driver was assigned, it
// stops the location stream (presence.Store.ClearActiveTrip) same as
// tripstart.Notifier does on trip start, then broadcasts a cancellation
// notice to Redis so every gateway instance can push it to whichever local
// connections it holds -- rider, assigned driver, and/or any drivers whose
// pending offer was withdrawn.
type Notifier struct {
	store *presence.Store
	bus   *presence.Bus
}

func NewNotifier(store *presence.Store, bus *presence.Bus) *Notifier {
	return &Notifier{store: store, bus: bus}
}

func (n *Notifier) HandleRideCancelledEvent(ctx context.Context, event events.RideCancelledV1) error {
	if event.DriverID != "" {
		// Best-effort: a Redis hiccup here must not block telling the rider
		// and driver the trip was cancelled -- the mapping self-heals via TTL
		// either way.
		if err := n.store.ClearActiveTrip(ctx, event.DriverID); err != nil {
			log.Printf("websocket_gateway: clear active trip failed driver_id=%s request_id=%s: %v", event.DriverID, event.RequestID, err)
		}
	}

	broadcast := TripCancelledBroadcast{
		RequestID:               event.RequestID,
		TripID:                  event.TripID,
		OngoingTripID:           event.OngoingTripID,
		RiderID:                 event.RiderID,
		DriverID:                event.DriverID,
		Stage:                   event.Stage,
		WithdrawnOfferDriverIDs: event.WithdrawnOfferDriverIDs,
		CancelledBy:             event.CancelledBy,
		CancelledAt:             event.CancelledAt,
	}

	payload, err := json.Marshal(broadcast)
	if err != nil {
		return fmt.Errorf("marshal trip cancelled broadcast request_id=%s: %w", event.RequestID, err)
	}

	if err := n.bus.Publish(ctx, payload); err != nil {
		return fmt.Errorf("publish trip cancelled broadcast request_id=%s: %w", event.RequestID, err)
	}
	return nil
}
