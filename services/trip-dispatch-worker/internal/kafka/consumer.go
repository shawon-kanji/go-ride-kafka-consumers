package kafka

import (
	"context"
	"log"
)

type Consumer interface {
	Start(ctx context.Context) error
}

type NoopConsumer struct {
	service string
	topic   string
}

func NewNoopConsumer(service string, topic string) *NoopConsumer {
	return &NoopConsumer{service: service, topic: topic}
}

func (c *NoopConsumer) Start(ctx context.Context) error {
	log.Printf("noop consumer started service=%s topic=%s; replace with real dispatch consumer", c.service, c.topic)
	<-ctx.Done()
	log.Printf("noop consumer stopping service=%s", c.service)
	return nil
}
