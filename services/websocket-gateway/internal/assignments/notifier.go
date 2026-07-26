package assignments

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/presence"
	"go-ride-kafka-consumers/services/websocket-gateway/pkg/events"
)

// Notifier broadcasts a RideAssignedV1 event to Redis so every gateway
// instance can filter it against its own local rider hub — same
// broadcast-and-filter design as internal/offers.Notifier for job offers.
// It also records the driver->rider active-trip mapping the tracking
// package's location pipeline depends on to filter the location firehose.
type Notifier struct {
	bus   *presence.Bus
	store *presence.Store
}

func NewNotifier(bus *presence.Bus, store *presence.Store) *Notifier {
	return &Notifier{bus: bus, store: store}
}

func (n *Notifier) HandleRideAssignedEvent(ctx context.Context, event events.RideAssignedV1) error {
	// Best-effort: a failure here must not block the ride_assigned push
	// itself — it only means this trip won't get live location updates.
	if err := n.store.SetActiveTrip(ctx, event.DriverID, presence.ActiveTrip{
		RiderID:       event.RiderID,
		TripID:        event.TripID,
		OngoingTripID: event.OngoingTripID,
		RequestID:     event.RequestID,
	}); err != nil {
		log.Printf("websocket_gateway: set active trip failed driver_id=%s request_id=%s: %v", event.DriverID, event.RequestID, err)
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal ride assigned event request_id=%s: %w", event.RequestID, err)
	}

	if err := n.bus.Publish(ctx, payload); err != nil {
		return fmt.Errorf("publish ride assigned event request_id=%s: %w", event.RequestID, err)
	}
	return nil
}
