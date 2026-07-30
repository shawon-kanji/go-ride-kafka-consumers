package tripcancel

import (
	"context"
	"encoding/json"
	"log"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/ws"

	"github.com/google/uuid"
)

// Deliverer handles every trip-cancelled broadcast received from Redis on
// this instance. Unlike every other trip-event pipeline (rider-only), a
// cancellation can have up to three audiences: the rider (always), the
// assigned driver if one existed, and any drivers whose still-pending offer
// was withdrawn. Each lookup is a no-op unless this instance happens to hold
// that connection.
type Deliverer struct {
	riderHub *ws.RiderHub
	hub      *ws.Hub
}

func NewDeliverer(riderHub *ws.RiderHub, hub *ws.Hub) *Deliverer {
	return &Deliverer{riderHub: riderHub, hub: hub}
}

func (d *Deliverer) HandleBroadcast(ctx context.Context, payload []byte) {
	var broadcast TripCancelledBroadcast
	if err := json.Unmarshal(payload, &broadcast); err != nil {
		log.Printf("websocket_gateway: discard unparseable trip cancelled broadcast: %v", err)
		return
	}

	d.deliverToRider(broadcast)
	d.deliverToDriver(broadcast)
	d.deliverToWithdrawnOfferDrivers(broadcast)
}

func (d *Deliverer) deliverToRider(broadcast TripCancelledBroadcast) {
	riderID, err := uuid.Parse(broadcast.RiderID)
	if err != nil {
		log.Printf("websocket_gateway: discard trip cancelled broadcast with invalid rider_id=%q request_id=%s: %v", broadcast.RiderID, broadcast.RequestID, err)
		return
	}

	conns := d.riderHub.ConnectionsForRider(riderID)
	if len(conns) == 0 {
		return
	}

	message := ws.NewTripCancelledMessage(ws.TripCancelledParams{
		RequestID:     broadcast.RequestID,
		TripID:        broadcast.TripID,
		OngoingTripID: broadcast.OngoingTripID,
		DriverID:      broadcast.DriverID,
		Stage:         broadcast.Stage,
		CancelledBy:   broadcast.CancelledBy,
		CancelledAt:   broadcast.CancelledAt,
	})
	payloadOut, err := json.Marshal(message)
	if err != nil {
		log.Printf("websocket_gateway: marshal trip cancelled message failed request_id=%s: %v", broadcast.RequestID, err)
		return
	}

	for _, conn := range conns {
		conn.Send(payloadOut)
	}
}

func (d *Deliverer) deliverToDriver(broadcast TripCancelledBroadcast) {
	if broadcast.DriverID == "" {
		return
	}

	driverID, err := uuid.Parse(broadcast.DriverID)
	if err != nil {
		log.Printf("websocket_gateway: discard trip cancelled broadcast with invalid driver_id=%q request_id=%s: %v", broadcast.DriverID, broadcast.RequestID, err)
		return
	}

	conns := d.hub.ConnectionsForDriver(driverID)
	if len(conns) == 0 {
		return
	}

	message := ws.NewTripCancelledMessage(ws.TripCancelledParams{
		RequestID:     broadcast.RequestID,
		TripID:        broadcast.TripID,
		OngoingTripID: broadcast.OngoingTripID,
		DriverID:      broadcast.DriverID,
		Stage:         broadcast.Stage,
		CancelledBy:   broadcast.CancelledBy,
		CancelledAt:   broadcast.CancelledAt,
	})
	payloadOut, err := json.Marshal(message)
	if err != nil {
		log.Printf("websocket_gateway: marshal trip cancelled message failed request_id=%s: %v", broadcast.RequestID, err)
		return
	}

	for _, conn := range conns {
		conn.Send(payloadOut)
	}
}

func (d *Deliverer) deliverToWithdrawnOfferDrivers(broadcast TripCancelledBroadcast) {
	if len(broadcast.WithdrawnOfferDriverIDs) == 0 {
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

	for _, rawDriverID := range broadcast.WithdrawnOfferDriverIDs {
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
