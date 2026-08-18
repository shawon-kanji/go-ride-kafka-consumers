package offers

import (
	"context"
	"encoding/json"
	"fmt"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/presence"

	"github.com/shawon-kanji/go-ride-utils/events"
)

// WithdrawnBroadcast is the Redis pub/sub wire format for a
// JobOfferWithdrawnV1 batch — just the driver IDs a gateway instance needs
// to route to, since ws.OfferWithdrawnMessage (shared with the
// rider-cancellation withdrawal path in tripcancel.Deliverer) carries only
// request_id/trip_id, not job_offer_id.
type WithdrawnBroadcast struct {
	RequestID string   `json:"request_id"`
	TripID    string   `json:"trip_id"`
	DriverIDs []string `json:"driver_ids"`
}

// WithdrawnNotifier broadcasts a JobOfferWithdrawnV1 batch to Redis, mirroring
// Notifier's whole-batch-not-per-driver approach.
type WithdrawnNotifier struct {
	bus *presence.Bus
}

func NewWithdrawnNotifier(bus *presence.Bus) *WithdrawnNotifier {
	return &WithdrawnNotifier{bus: bus}
}

func (n *WithdrawnNotifier) HandleJobOfferWithdrawnEvent(ctx context.Context, event events.JobOfferWithdrawnV1) error {
	driverIDs := make([]string, len(event.Offers))
	for i, o := range event.Offers {
		driverIDs[i] = o.DriverID
	}

	broadcast := WithdrawnBroadcast{
		RequestID: event.RequestID,
		TripID:    event.TripID,
		DriverIDs: driverIDs,
	}

	payload, err := json.Marshal(broadcast)
	if err != nil {
		return fmt.Errorf("marshal job offer withdrawn broadcast request_id=%s: %w", event.RequestID, err)
	}

	if err := n.bus.Publish(ctx, payload); err != nil {
		return fmt.Errorf("publish job offer withdrawn broadcast request_id=%s: %w", event.RequestID, err)
	}
	return nil
}
