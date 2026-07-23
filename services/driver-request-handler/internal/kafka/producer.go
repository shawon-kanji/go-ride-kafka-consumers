package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go-ride-kafka-consumers/services/driver-request-handler/internal/config"
	"go-ride-kafka-consumers/services/driver-request-handler/pkg/events"

	kafkago "github.com/segmentio/kafka-go"
)

type Producer interface {
	PublishRideAssigned(ctx context.Context, event events.RideAssignedV1) error
	Close() error
}

type RideAssignedProducer struct {
	writer *kafkago.Writer
	topic  string
}

func NewRideAssignedProducer(cfg config.Config) *RideAssignedProducer {
	writer := &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.KafkaBrokers...),
		Topic:                  cfg.AssignedTopic,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireOne,
		BatchTimeout:           50 * time.Millisecond,
		WriteTimeout:           5 * time.Second,
		ReadTimeout:            5 * time.Second,
		AllowAutoTopicCreation: false,
	}

	return &RideAssignedProducer{
		writer: writer,
		topic:  cfg.AssignedTopic,
	}
}

func (p *RideAssignedProducer) PublishRideAssigned(ctx context.Context, event events.RideAssignedV1) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal ride assigned event: %w", err)
	}

	message := kafkago.Message{
		Key:   []byte(event.RequestID),
		Value: payload,
		Time:  event.PublishedAt,
	}

	if err := p.writer.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("write kafka message topic=%s: %w", p.topic, err)
	}

	return nil
}

func (p *RideAssignedProducer) Close() error {
	return p.writer.Close()
}
