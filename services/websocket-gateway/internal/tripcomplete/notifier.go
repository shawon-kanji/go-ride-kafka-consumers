package tripcomplete

import (
	"context"
	"encoding/json"
	"fmt"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/presence"
	"go-ride-kafka-consumers/services/websocket-gateway/pkg/events"
)

// Notifier reacts to a RideCompletedV1 by broadcasting a trip-completed
// notice to Redis. Like tripend.Notifier, no presence.Store interaction is
// needed here — the location-stream filter was already cleared at
// trip-start.
type Notifier struct {
	bus *presence.Bus
}

func NewNotifier(bus *presence.Bus) *Notifier {
	return &Notifier{bus: bus}
}

func (n *Notifier) HandleRideCompletedEvent(ctx context.Context, event events.RideCompletedV1) error {
	broadcast := TripCompletedBroadcast{
		RequestID:          event.RequestID,
		TripID:             event.TripID,
		OngoingTripID:      event.OngoingTripID,
		RiderID:            event.RiderID,
		DriverID:           event.DriverID,
		FinalFare:          event.FinalFare,
		CurrencyCode:       event.CurrencyCode,
		PaymentCollectedAt: event.PaymentCollectedAt,
	}

	payload, err := json.Marshal(broadcast)
	if err != nil {
		return fmt.Errorf("marshal trip completed broadcast request_id=%s: %w", event.RequestID, err)
	}

	if err := n.bus.Publish(ctx, payload); err != nil {
		return fmt.Errorf("publish trip completed broadcast request_id=%s: %w", event.RequestID, err)
	}
	return nil
}
