package offers

import (
	"context"
	"encoding/json"
	"log"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/ws"

	"github.com/google/uuid"
)

// WithdrawnDeliverer handles every job-offer-withdrawn broadcast received
// from Redis on this instance — the accept-path counterpart to
// tripcancel.Deliverer's deliverToWithdrawnOfferDrivers, reusing the exact
// same ws.OfferWithdrawnMessage shape so the driver app handles "taken by
// another driver" identically regardless of which path caused it.
type WithdrawnDeliverer struct {
	hub *ws.Hub
}

func NewWithdrawnDeliverer(hub *ws.Hub) *WithdrawnDeliverer {
	return &WithdrawnDeliverer{hub: hub}
}

func (d *WithdrawnDeliverer) HandleBroadcast(ctx context.Context, payload []byte) {
	var broadcast WithdrawnBroadcast
	if err := json.Unmarshal(payload, &broadcast); err != nil {
		log.Printf("websocket_gateway: discard unparseable job offer withdrawn broadcast: %v", err)
		return
	}

	message := ws.NewOfferWithdrawnMessage(ws.OfferWithdrawnParams{
		RequestID: broadcast.RequestID,
		TripID:    broadcast.TripID,
	})
	payloadOut, err := json.Marshal(message)
	if err != nil {
		log.Printf("websocket_gateway: marshal offer withdrawn message failed request_id=%s: %v", broadcast.RequestID, err)
		return
	}

	for _, rawDriverID := range broadcast.DriverIDs {
		driverID, err := uuid.Parse(rawDriverID)
		if err != nil {
			log.Printf("websocket_gateway: skip withdrawn offer push, invalid driver_id=%q request_id=%s: %v", rawDriverID, broadcast.RequestID, err)
			continue
		}

		conns := d.hub.ConnectionsForDriver(driverID)
		for _, conn := range conns {
			conn.Send(payloadOut)
		}
	}
}
