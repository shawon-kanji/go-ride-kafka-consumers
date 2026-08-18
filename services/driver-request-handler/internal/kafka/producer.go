package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/shawon-kanji/go-ride-utils/events"
	"go-ride-kafka-consumers/services/driver-request-handler/internal/config"

	kafkago "github.com/segmentio/kafka-go"
)

type Producer interface {
	PublishRideAssigned(ctx context.Context, event events.RideAssignedV1) error
	PublishRideStarted(ctx context.Context, event events.RideStartedV1) error
	PublishRideEnded(ctx context.Context, event events.RideEndedV1) error
	PublishRideCompleted(ctx context.Context, event events.RideCompletedV1) error
	PublishRideCancelled(ctx context.Context, event events.RideCancelledV1) error
	PublishJobOfferWithdrawn(ctx context.Context, event events.JobOfferWithdrawnV1) error
	Close() error
}

type RideAssignedProducer struct {
	writer          *kafkago.Writer
	topic           string
	startedWriter   *kafkago.Writer
	startedTopic    string
	endedWriter     *kafkago.Writer
	endedTopic      string
	completedWriter *kafkago.Writer
	completedTopic  string
	cancelledWriter *kafkago.Writer
	cancelledTopic  string
	withdrawnWriter *kafkago.Writer
	withdrawnTopic  string
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

	endedWriter := &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.KafkaBrokers...),
		Topic:                  cfg.EndedTopic,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireOne,
		BatchTimeout:           50 * time.Millisecond,
		WriteTimeout:           5 * time.Second,
		ReadTimeout:            5 * time.Second,
		AllowAutoTopicCreation: false,
	}

	completedWriter := &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.KafkaBrokers...),
		Topic:                  cfg.CompletedTopic,
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

	withdrawnWriter := &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.KafkaBrokers...),
		Topic:                  cfg.WithdrawnTopic,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireOne,
		BatchTimeout:           50 * time.Millisecond,
		WriteTimeout:           5 * time.Second,
		ReadTimeout:            5 * time.Second,
		AllowAutoTopicCreation: false,
	}

	return &RideAssignedProducer{
		writer:          writer,
		topic:           cfg.AssignedTopic,
		startedWriter:   startedWriter,
		startedTopic:    cfg.StartedTopic,
		endedWriter:     endedWriter,
		endedTopic:      cfg.EndedTopic,
		completedWriter: completedWriter,
		completedTopic:  cfg.CompletedTopic,
		cancelledWriter: cancelledWriter,
		cancelledTopic:  cfg.CancelledTopic,
		withdrawnWriter: withdrawnWriter,
		withdrawnTopic:  cfg.WithdrawnTopic,
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

func (p *RideAssignedProducer) PublishRideEnded(ctx context.Context, event events.RideEndedV1) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal ride ended event: %w", err)
	}

	message := kafkago.Message{
		Key:   []byte(event.RequestID),
		Value: payload,
		Time:  event.PublishedAt,
	}

	if err := p.endedWriter.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("write kafka message topic=%s: %w", p.endedTopic, err)
	}

	return nil
}

func (p *RideAssignedProducer) PublishRideCompleted(ctx context.Context, event events.RideCompletedV1) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal ride completed event: %w", err)
	}

	message := kafkago.Message{
		Key:   []byte(event.RequestID),
		Value: payload,
		Time:  event.PublishedAt,
	}

	if err := p.completedWriter.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("write kafka message topic=%s: %w", p.completedTopic, err)
	}

	return nil
}

func (p *RideAssignedProducer) PublishRideCancelled(ctx context.Context, event events.RideCancelledV1) error {
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

func (p *RideAssignedProducer) PublishJobOfferWithdrawn(ctx context.Context, event events.JobOfferWithdrawnV1) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal job offer withdrawn event: %w", err)
	}

	message := kafkago.Message{
		Key:   []byte(event.RequestID),
		Value: payload,
		Time:  event.PublishedAt,
	}

	if err := p.withdrawnWriter.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("write kafka message topic=%s: %w", p.withdrawnTopic, err)
	}

	return nil
}

func (p *RideAssignedProducer) Close() error {
	if err := p.writer.Close(); err != nil {
		return err
	}
	if err := p.startedWriter.Close(); err != nil {
		return err
	}
	if err := p.endedWriter.Close(); err != nil {
		return err
	}
	if err := p.completedWriter.Close(); err != nil {
		return err
	}
	if err := p.cancelledWriter.Close(); err != nil {
		return err
	}
	return p.withdrawnWriter.Close()
}
