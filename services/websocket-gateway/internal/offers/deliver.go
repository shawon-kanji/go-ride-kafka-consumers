package offers

import (
	"context"
	"encoding/json"
	"log"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/ws"
	"github.com/shawon-kanji/go-ride-utils/events"

	"github.com/google/uuid"
)

// Deliverer handles every job offer batch broadcast received from Redis on
// this instance. For each offer entry in the batch, most calls are a no-op:
// only the instance actually holding that entry's driver connection has
// anything to deliver.
type Deliverer struct {
	hub   *ws.Hub
	store *Store
}

func NewDeliverer(hub *ws.Hub, store *Store) *Deliverer {
	return &Deliverer{hub: hub, store: store}
}

func (d *Deliverer) HandleBroadcast(ctx context.Context, payload []byte) {
	var event events.JobOfferV1
	if err := json.Unmarshal(payload, &event); err != nil {
		log.Printf("websocket_gateway: discard unparseable job offer broadcast: %v", err)
		return
	}

	for _, entry := range event.Offers {
		driverID, err := uuid.Parse(entry.DriverID)
		if err != nil {
			log.Printf("websocket_gateway: discard offer with invalid driver_id=%q job_offer_id=%s: %v", entry.DriverID, entry.JobOfferID, err)
			continue
		}

		conns := d.hub.ConnectionsForDriver(driverID)
		if len(conns) == 0 {
			continue
		}

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
			log.Printf("websocket_gateway: marshal job offer message failed job_offer_id=%s: %v", entry.JobOfferID, err)
			continue
		}

		delivered := false
		for _, conn := range conns {
			if conn.Send(payload) {
				delivered = true
			}
		}
		if !delivered {
			continue
		}

		jobOfferID, err := uuid.Parse(entry.JobOfferID)
		if err != nil {
			log.Printf("websocket_gateway: invalid job_offer_id=%q: %v", entry.JobOfferID, err)
			continue
		}
		if err := d.store.MarkSent(ctx, jobOfferID); err != nil {
			log.Printf("websocket_gateway: mark sent failed job_offer_id=%s: %v", entry.JobOfferID, err)
		}
	}
}
