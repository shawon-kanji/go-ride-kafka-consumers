package tripend

import (
	"context"
	"encoding/json"
	"fmt"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/presence"
	"go-ride-kafka-consumers/services/websocket-gateway/pkg/events"
)

// Notifier reacts to a RideEndedV1 by broadcasting a trip-ended notice to
// Redis. Unlike tripstart.Notifier there is no presence.Store interaction —
// the location-stream filter was already cleared at trip-start (Phase 7d).
type Notifier struct {
	bus *presence.Bus
}

func NewNotifier(bus *presence.Bus) *Notifier {
	return &Notifier{bus: bus}
}

func (n *Notifier) HandleRideEndedEvent(ctx context.Context, event events.RideEndedV1) error {
	broadcast := TripEndedBroadcast{
		RequestID:     event.RequestID,
		TripID:        event.TripID,
		OngoingTripID: event.OngoingTripID,
		RiderID:       event.RiderID,
		DriverID:      event.DriverID,
		FinalFare:     event.FinalFare,
		CurrencyCode:  event.CurrencyCode,
		EndedAt:       event.EndedAt,
	}

	payload, err := json.Marshal(broadcast)
	if err != nil {
		return fmt.Errorf("marshal trip ended broadcast request_id=%s: %w", event.RequestID, err)
	}

	if err := n.bus.Publish(ctx, payload); err != nil {
		return fmt.Errorf("publish trip ended broadcast request_id=%s: %w", event.RequestID, err)
	}
	return nil
}
