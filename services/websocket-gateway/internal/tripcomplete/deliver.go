package tripcomplete

import (
	"context"
	"encoding/json"
	"log"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/ws"

	"github.com/google/uuid"
)

// Deliverer handles every trip-completed broadcast received from Redis on
// this instance. It's a no-op unless this instance happens to hold the
// rider's live websocket connection.
type Deliverer struct {
	riderHub *ws.RiderHub
}

func NewDeliverer(riderHub *ws.RiderHub) *Deliverer {
	return &Deliverer{riderHub: riderHub}
}

func (d *Deliverer) HandleBroadcast(ctx context.Context, payload []byte) {
	var broadcast TripCompletedBroadcast
	if err := json.Unmarshal(payload, &broadcast); err != nil {
		log.Printf("websocket_gateway: discard unparseable trip completed broadcast: %v", err)
		return
	}

	riderID, err := uuid.Parse(broadcast.RiderID)
	if err != nil {
		log.Printf("websocket_gateway: discard trip completed broadcast with invalid rider_id=%q request_id=%s: %v", broadcast.RiderID, broadcast.RequestID, err)
		return
	}

	conns := d.riderHub.ConnectionsForRider(riderID)
	if len(conns) == 0 {
		return
	}

	message := ws.NewTripCompletedMessage(ws.TripCompletedParams{
		RequestID:          broadcast.RequestID,
		TripID:             broadcast.TripID,
		OngoingTripID:      broadcast.OngoingTripID,
		DriverID:           broadcast.DriverID,
		FinalFare:          broadcast.FinalFare,
		CurrencyCode:       broadcast.CurrencyCode,
		PaymentCollectedAt: broadcast.PaymentCollectedAt,
	})

	payloadOut, err := json.Marshal(message)
	if err != nil {
		log.Printf("websocket_gateway: marshal trip completed message failed request_id=%s: %v", broadcast.RequestID, err)
		return
	}

	for _, conn := range conns {
		conn.Send(payloadOut)
	}
}
