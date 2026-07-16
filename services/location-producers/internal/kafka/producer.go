package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go-ride-kafka-consumers/services/location-producers/internal/config"
	"go-ride-kafka-consumers/services/location-producers/pkg/events"

	kafkago "github.com/segmentio/kafka-go"
)

type Producer interface {
	PublishDriverLocation(ctx context.Context, event events.DriverLocationUpdatedV1) error
	Close() error
}

type DriverLocationProducer struct {
	writer *kafkago.Writer
	topic  string
}

func NewDriverLocationProducer(cfg config.Config) *DriverLocationProducer {
	writer := &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.KafkaBrokers...),
		Topic:                  cfg.KafkaTopic,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireOne,
		BatchTimeout:           50 * time.Millisecond,
		WriteTimeout:           5 * time.Second,
		ReadTimeout:            5 * time.Second,
		AllowAutoTopicCreation: false,
	}

	return &DriverLocationProducer{
		writer: writer,
		topic:  cfg.KafkaTopic,
	}
}

func (p *DriverLocationProducer) PublishDriverLocation(ctx context.Context, event events.DriverLocationUpdatedV1) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal driver location event: %w", err)
	}

	message := kafkago.Message{
		Key:   []byte(event.DriverID),
		Value: payload,
		Time:  event.PublishedAt,
	}

	if err := p.writer.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("write kafka message topic=%s: %w", p.topic, err)
	}

	return nil
}

func (p *DriverLocationProducer) Close() error {
	return p.writer.Close()
}
