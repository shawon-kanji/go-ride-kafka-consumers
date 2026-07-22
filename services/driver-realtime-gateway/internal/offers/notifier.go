package offers

import (
	"context"
	"encoding/json"
	"fmt"

	"go-ride-kafka-consumers/services/driver-realtime-gateway/internal/presence"
	"go-ride-kafka-consumers/services/driver-realtime-gateway/pkg/events"
)

// Notifier broadcasts a JobOfferV1 batch to Redis once per dispatch attempt,
// rather than once per driver offer. Every gateway instance receives the
// whole batch and filters it against its own local hub — a driver's live
// connection lives on exactly one instance at a time, so this is strictly
// less work than N separate publishes for the same total delivery coverage.
type Notifier struct {
	bus *presence.Bus
}

func NewNotifier(bus *presence.Bus) *Notifier {
	return &Notifier{bus: bus}
}

func (n *Notifier) HandleJobOfferEvent(ctx context.Context, event events.JobOfferV1) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal job offer batch request_id=%s: %w", event.RequestID, err)
	}

	if err := n.bus.Publish(ctx, payload); err != nil {
		return fmt.Errorf("publish job offer batch request_id=%s: %w", event.RequestID, err)
	}
	return nil
}
