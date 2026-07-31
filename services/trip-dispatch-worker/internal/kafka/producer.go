package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/shawon-kanji/go-ride-utils/events"
	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/config"

	kafkago "github.com/segmentio/kafka-go"
)

type Producer interface {
	PublishJobOffer(ctx context.Context, event events.JobOfferV1) error
	Close() error
}

type TripDispatcherProducer struct {
	writer *kafkago.Writer
	topic  string
}

func NewTripDispatcherProducer(cfg config.Config) *TripDispatcherProducer {
	writer := &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.KafkaBrokers...),
		Topic:                  cfg.OfferCreatedTopic,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireOne,
		BatchTimeout:           50 * time.Millisecond,
		WriteTimeout:           5 * time.Second,
		ReadTimeout:            5 * time.Second,
		AllowAutoTopicCreation: false,
	}

	return &TripDispatcherProducer{
		writer: writer,
		topic:  cfg.OfferCreatedTopic,
	}
}

func (p *TripDispatcherProducer) PublishJobOffer(ctx context.Context, event events.JobOfferV1) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal job offer event: %w", err)
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

func (p *TripDispatcherProducer) Close() error {
	return p.writer.Close()
}
