package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/shawon-kanji/go-ride-utils/events"
	"go-ride-kafka-consumers/services/cab-request-handler/internal/config"

	kafkago "github.com/segmentio/kafka-go"
)

type Producer interface {
	PublishRideRequested(ctx context.Context, event events.RideRequestedV1) error
	PublishRideCancelled(ctx context.Context, event events.RideCancelledV1) error
	Close() error
}

type RideRequestProducer struct {
	writer          *kafkago.Writer
	topic           string
	cancelledWriter *kafkago.Writer
	cancelledTopic  string
}

func NewRideRequestProducer(cfg config.Config) *RideRequestProducer {
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

	cancelledWriter := &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.KafkaBrokers...),
		Topic:                  cfg.CancelledTopic,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireOne,
		BatchTimeout:           50 * time.Millisecond,
		WriteTimeout:           5 * time.Second,
		ReadTimeout:            5 * time.Second,
		AllowAutoTopicCreation: false,
	}

	return &RideRequestProducer{
		writer:          writer,
		topic:           cfg.KafkaTopic,
		cancelledWriter: cancelledWriter,
		cancelledTopic:  cfg.CancelledTopic,
	}
}

func (p *RideRequestProducer) PublishRideRequested(ctx context.Context, event events.RideRequestedV1) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal ride requested event: %w", err)
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

func (p *RideRequestProducer) PublishRideCancelled(ctx context.Context, event events.RideCancelledV1) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal ride cancelled event: %w", err)
	}

	message := kafkago.Message{
		Key:   []byte(event.RequestID),
		Value: payload,
		Time:  event.PublishedAt,
	}

	if err := p.cancelledWriter.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("write kafka message topic=%s: %w", p.cancelledTopic, err)
	}

	return nil
}

func (p *RideRequestProducer) Close() error {
	if err := p.writer.Close(); err != nil {
		return err
	}
	return p.cancelledWriter.Close()
}
