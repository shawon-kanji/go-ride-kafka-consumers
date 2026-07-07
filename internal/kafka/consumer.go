package kafka

import (
	"context"
	"log"
)

type Consumer interface {
	Start(ctx context.Context) error
}

type NoopConsumer struct{}

func NewNoopConsumer() *NoopConsumer {
	return &NoopConsumer{}
}

func (c *NoopConsumer) Start(ctx context.Context) error {
	log.Printf("noop consumer started; replace with real Kafka consumer implementation")
	<-ctx.Done()
	log.Printf("noop consumer stopping")
	return nil
}
