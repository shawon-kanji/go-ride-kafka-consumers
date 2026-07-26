package tracking

import (
	"context"
	"encoding/json"
	"log"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/ws"

	"github.com/google/uuid"
)

// Deliverer handles every location broadcast received from Redis on this
// instance. It's a no-op unless this instance happens to hold the rider's
// live websocket connection. It never touches presence.Store — Notifier
// already resolved the driver->rider mapping once, so there's exactly one
// Redis lookup per forwarded ping, not one per gateway instance.
type Deliverer struct {
	riderHub *ws.RiderHub
}

func NewDeliverer(riderHub *ws.RiderHub) *Deliverer {
	return &Deliverer{riderHub: riderHub}
}

func (d *Deliverer) HandleBroadcast(ctx context.Context, payload []byte) {
	var broadcast LocationBroadcast
	if err := json.Unmarshal(payload, &broadcast); err != nil {
		log.Printf("websocket_gateway: discard unparseable location broadcast: %v", err)
		return
	}

	riderID, err := uuid.Parse(broadcast.RiderID)
	if err != nil {
		log.Printf("websocket_gateway: discard location broadcast with invalid rider_id=%q driver_id=%s: %v", broadcast.RiderID, broadcast.DriverID, err)
		return
	}

	conns := d.riderHub.ConnectionsForRider(riderID)
	if len(conns) == 0 {
		return
	}

	message := ws.NewDriverLocationMessage(ws.DriverLocationParams{
		TripID:        broadcast.TripID,
		OngoingTripID: broadcast.OngoingTripID,
		DriverID:      broadcast.DriverID,
		Latitude:      broadcast.Latitude,
		Longitude:     broadcast.Longitude,
		AccuracyM:     broadcast.AccuracyM,
		EventTime:     broadcast.EventTime,
	})

	payloadOut, err := json.Marshal(message)
	if err != nil {
		log.Printf("websocket_gateway: marshal driver location message failed driver_id=%s: %v", broadcast.DriverID, err)
		return
	}

	for _, conn := range conns {
		conn.Send(payloadOut)
	}
}
