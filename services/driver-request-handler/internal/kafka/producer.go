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
	PublishRideStarted(ctx context.Context, event events.RideStartedV1) error
	Close() error
}

type RideAssignedProducer struct {
	writer        *kafkago.Writer
	topic         string
	startedWriter *kafkago.Writer
	startedTopic  string
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

	startedWriter := &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.KafkaBrokers...),
		Topic:                  cfg.StartedTopic,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireOne,
		BatchTimeout:           50 * time.Millisecond,
		WriteTimeout:           5 * time.Second,
		ReadTimeout:            5 * time.Second,
		AllowAutoTopicCreation: false,
	}

	return &RideAssignedProducer{
		writer:        writer,
		topic:         cfg.AssignedTopic,
		startedWriter: startedWriter,
		startedTopic:  cfg.StartedTopic,
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

func (p *RideAssignedProducer) PublishRideStarted(ctx context.Context, event events.RideStartedV1) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal ride started event: %w", err)
	}

	message := kafkago.Message{
		Key:   []byte(event.RequestID),
		Value: payload,
		Time:  event.PublishedAt,
	}

	if err := p.startedWriter.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("write kafka message topic=%s: %w", p.startedTopic, err)
	}

	return nil
}

func (p *RideAssignedProducer) Close() error {
	if err := p.writer.Close(); err != nil {
		return err
	}
	return p.startedWriter.Close()
}
