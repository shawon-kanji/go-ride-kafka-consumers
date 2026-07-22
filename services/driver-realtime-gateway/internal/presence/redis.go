// Package presence provides the cross-instance routing layer for the
// realtime gateway: any gateway instance's Kafka consumer can receive a job
// offer batch for drivers connected to *other* instances, so Redis pub/sub
// fans the whole batch out to all instances, and each instance filters it
// against its own local connections — only the one actually holding a given
// driver's websocket does anything with that entry. This is what lets the
// gateway run as multiple pods (e.g. on EKS) without sticky routing, and
// without needing to know in advance which instance holds which driver.
package presence

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

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

// Publish broadcasts payload to every subscribed gateway instance as-is —
// this package doesn't parse it, so callers are free to publish a whole
// batch (e.g. a JobOfferV1) rather than one message per driver.
func (b *Bus) Publish(ctx context.Context, payload []byte) error {
	if err := b.client.Publish(ctx, b.channel, payload).Err(); err != nil {
		return fmt.Errorf("publish presence broadcast: %w", err)
	}
	return nil
}

// Subscribe blocks, delivering each broadcast's raw payload to handler,
// until ctx is canceled. Every gateway instance subscribes to the same
// channel and receives every broadcast.
func (b *Bus) Subscribe(ctx context.Context, handler func(payload []byte)) error {
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
			handler([]byte(msg.Payload))
		}
	}
}

func (b *Bus) Close() error {
	return b.client.Close()
}
