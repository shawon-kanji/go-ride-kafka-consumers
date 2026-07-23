package assignments

import (
	"context"
	"encoding/json"
	"fmt"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/presence"
	"go-ride-kafka-consumers/services/websocket-gateway/pkg/events"
)

// Notifier broadcasts a RideAssignedV1 event to Redis so every gateway
// instance can filter it against its own local rider hub — same
// broadcast-and-filter design as internal/offers.Notifier for job offers.
type Notifier struct {
	bus *presence.Bus
}

func NewNotifier(bus *presence.Bus) *Notifier {
	return &Notifier{bus: bus}
}

func (n *Notifier) HandleRideAssignedEvent(ctx context.Context, event events.RideAssignedV1) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal ride assigned event request_id=%s: %w", event.RequestID, err)
	}

	if err := n.bus.Publish(ctx, payload); err != nil {
		return fmt.Errorf("publish ride assigned event request_id=%s: %w", event.RequestID, err)
	}
	return nil
}
