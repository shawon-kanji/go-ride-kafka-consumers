// Package presence provides the cross-instance routing layer for the
// realtime gateway: any gateway instance's Kafka consumer can receive a job
// offer for a driver connected to a *different* instance, so Redis pub/sub
// fans the notification out to all instances, and only the one actually
// holding that driver's websocket connection acts on it. This is what lets
// the gateway run as multiple pods (e.g. on EKS) without sticky routing.
package presence

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

// Notification is the pub/sub envelope. Payload is opaque here (an
// already-marshaled internal/ws.JobOfferMessage) so this package stays
// decoupled from the websocket wire format.
type Notification struct {
	DriverID string          `json:"driver_id"`
	Payload  json.RawMessage `json:"payload"`
}

type Bus struct {
	client  *redis.Client
	channel string
}

func NewBus(addr, password string, db int, channel string) *Bus {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	return &Bus{client: client, channel: channel}
}

func (b *Bus) Publish(ctx context.Context, driverID string, payload []byte) error {
	notification := Notification{DriverID: driverID, Payload: payload}
	data, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("marshal presence notification: %w", err)
	}
	if err := b.client.Publish(ctx, b.channel, data).Err(); err != nil {
		return fmt.Errorf("publish presence notification driver_id=%s: %w", driverID, err)
	}
	return nil
}

// Subscribe blocks, delivering each notification to handler, until ctx is
// canceled. Every gateway instance subscribes to the same channel.
func (b *Bus) Subscribe(ctx context.Context, handler func(Notification)) error {
	sub := b.client.Subscribe(ctx, b.channel)
	defer func() {
		if err := sub.Close(); err != nil {
			log.Printf("driver_realtime_gateway: close redis subscription: %v", err)
		}
	}()

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			var notification Notification
			if err := json.Unmarshal([]byte(msg.Payload), &notification); err != nil {
				log.Printf("driver_realtime_gateway: discard unparseable presence notification: %v", err)
				continue
			}
			handler(notification)
		}
	}
}

func (b *Bus) Close() error {
	return b.client.Close()
}
