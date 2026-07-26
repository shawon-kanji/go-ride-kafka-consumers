package assignments

import (
	"context"
	"encoding/json"
	"log"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/ws"
	"github.com/shawon-kanji/go-ride-utils/events"

	"github.com/google/uuid"
)

// Deliverer handles every ride-assigned broadcast received from Redis on
// this instance. It's a no-op unless this instance happens to hold the
// rider's live websocket connection.
type Deliverer struct {
	riderHub *ws.RiderHub
}

func NewDeliverer(riderHub *ws.RiderHub) *Deliverer {
	return &Deliverer{riderHub: riderHub}
}

func (d *Deliverer) HandleBroadcast(ctx context.Context, payload []byte) {
	var event events.RideAssignedV1
	if err := json.Unmarshal(payload, &event); err != nil {
		log.Printf("websocket_gateway: discard unparseable ride assigned broadcast: %v", err)
		return
	}

	riderID, err := uuid.Parse(event.RiderID)
	if err != nil {
		log.Printf("websocket_gateway: discard ride assigned event with invalid rider_id=%q request_id=%s: %v", event.RiderID, event.RequestID, err)
		return
	}

	conns := d.riderHub.ConnectionsForRider(riderID)
	if len(conns) == 0 {
		return
	}

	message := ws.NewRideAssignedMessage(ws.RideAssignedParams{
		RequestID:     event.RequestID,
		TripID:        event.TripID,
		OngoingTripID: event.OngoingTripID,
		DriverID:      event.DriverID,
		DriverName:    event.DriverName,
		DriverLat:     event.DriverLat,
		DriverLng:     event.DriverLng,
		PickupLat:     event.PickupLat,
		PickupLng:     event.PickupLng,
		DropoffLat:    event.DropoffLat,
		DropoffLng:    event.DropoffLng,
		VehicleColor:  event.VehicleColor,
		VehiclePlate:  event.VehiclePlate,
		VehicleModel:  event.VehicleModel,
		CorrelationID: event.CorrelationID,
	})

	payloadOut, err := json.Marshal(message)
	if err != nil {
		log.Printf("websocket_gateway: marshal ride assigned message failed request_id=%s: %v", event.RequestID, err)
		return
	}

	for _, conn := range conns {
		conn.Send(payloadOut)
	}
}
