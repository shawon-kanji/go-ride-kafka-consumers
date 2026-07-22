package offers

import (
	"context"
	"encoding/json"
	"fmt"

	"go-ride-kafka-consumers/services/driver-realtime-gateway/internal/presence"
	"go-ride-kafka-consumers/services/driver-realtime-gateway/internal/ws"
	"go-ride-kafka-consumers/services/driver-realtime-gateway/pkg/events"
)

// Notifier fans a JobOfferV1 batch out to Redis, one notification per driver
// offer, rather than pushing to the local hub directly — the Kafka consumer
// firing on this instance has no guarantee it's the instance holding any
// given driver's connection, so every offer goes through presence.Bus and
// only the instance that actually holds the connection acts on it.
type Notifier struct {
	bus *presence.Bus
}

func NewNotifier(bus *presence.Bus) *Notifier {
	return &Notifier{bus: bus}
}

func (n *Notifier) HandleJobOfferEvent(ctx context.Context, event events.JobOfferV1) error {
	for _, entry := range event.Offers {
		message := ws.NewJobOfferMessage(ws.JobOfferParams{
			JobOfferID:       entry.JobOfferID,
			RequestID:        event.RequestID,
			TripID:           event.TripID,
			OfferRank:        entry.OfferRank,
			PickupLat:        event.PickupLat,
			PickupLng:        event.PickupLng,
			DropoffLat:       event.DropoffLat,
			DropoffLng:       event.DropoffLng,
			EstimatedEarning: event.EstimatedEarning,
			CurrencyCode:     event.CurrencyCode,
			ExpiresAt:        entry.ExpiresAt,
			CorrelationID:    event.CorrelationID,
		})

		payload, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal job offer message job_offer_id=%s: %w", entry.JobOfferID, err)
		}

		if err := n.bus.Publish(ctx, entry.DriverID, payload); err != nil {
			return fmt.Errorf("publish job offer notification job_offer_id=%s: %w", entry.JobOfferID, err)
		}
	}
	return nil
}
