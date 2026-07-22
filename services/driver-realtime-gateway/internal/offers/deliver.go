package offers

import (
	"context"
	"encoding/json"
	"log"

	"go-ride-kafka-consumers/services/driver-realtime-gateway/internal/presence"
	"go-ride-kafka-consumers/services/driver-realtime-gateway/internal/ws"

	"github.com/google/uuid"
)

// Deliverer handles every presence.Notification received from Redis on this
// instance. Most calls are a no-op: only the instance actually holding the
// target driver's connection has anything to deliver.
type Deliverer struct {
	hub   *ws.Hub
	store *Store
}

func NewDeliverer(hub *ws.Hub, store *Store) *Deliverer {
	return &Deliverer{hub: hub, store: store}
}

func (d *Deliverer) HandleNotification(ctx context.Context, notification presence.Notification) {
	driverID, err := uuid.Parse(notification.DriverID)
	if err != nil {
		log.Printf("driver_realtime_gateway: discard notification with invalid driver_id=%q: %v", notification.DriverID, err)
		return
	}

	conns := d.hub.ConnectionsForDriver(driverID)
	if len(conns) == 0 {
		return
	}

	var message ws.JobOfferMessage
	if err := json.Unmarshal(notification.Payload, &message); err != nil {
		log.Printf("driver_realtime_gateway: discard unparseable notification payload driver_id=%s: %v", driverID, err)
		return
	}

	delivered := false
	for _, conn := range conns {
		if conn.Send(notification.Payload) {
			delivered = true
		}
	}
	if !delivered {
		return
	}

	jobOfferID, err := uuid.Parse(message.JobOfferID)
	if err != nil {
		log.Printf("driver_realtime_gateway: invalid job_offer_id in notification driver_id=%s: %v", driverID, err)
		return
	}
	if err := d.store.MarkSent(ctx, jobOfferID); err != nil {
		log.Printf("driver_realtime_gateway: mark sent failed job_offer_id=%s: %v", jobOfferID, err)
	}
}
