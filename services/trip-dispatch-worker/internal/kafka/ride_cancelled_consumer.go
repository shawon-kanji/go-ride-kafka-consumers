package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
	"github.com/shawon-kanji/go-ride-utils/events"
	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/config"
	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/dispatch"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

// RideCancelledConsumer reacts to driver-initiated cancellations of a trip
// still before its start (stage "assigned"), triggering an automatic
// redispatch so the rider isn't left dangling waiting for a replacement
// driver. Mid-trip ("in_progress") cancellations are terminate-only and are
// skipped here — no replacement driver can physically take over an
// in-progress trip.
type RideCancelledConsumer struct {
	reader  *kafkago.Reader
	service *dispatch.Service
	topic   string
	group   string
}

func NewRideCancelledConsumer(cfg config.Config, service *dispatch.Service) *RideCancelledConsumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.CancelledTopic,
		GroupID:  cfg.ConsumerGroup,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &RideCancelledConsumer{
		reader:  reader,
		service: service,
		topic:   cfg.CancelledTopic,
		group:   cfg.ConsumerGroup,
	}
}

func (c *RideCancelledConsumer) Start(ctx context.Context) error {
	defer func() {
		if err := c.reader.Close(); err != nil {
			log.Printf("close kafka reader topic=%s group=%s: %v", c.topic, c.group, err)
		}
	}()

	log.Printf("kafka consumer started topic=%s group=%s", c.topic, c.group)

	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				log.Printf("kafka consumer stopping topic=%s group=%s", c.topic, c.group)
				return nil
			}
			return fmt.Errorf("fetch kafka message: %w", err)
		}

		if err := c.handleMessage(ctx, message); err != nil {
			var invalidMessageErr *invalidMessageError
			if errors.As(err, &invalidMessageErr) {
				log.Printf("skip invalid ride cancelled message topic=%s partition=%d offset=%d: %v", message.Topic, message.Partition, message.Offset, err)
				if commitErr := c.reader.CommitMessages(ctx, message); commitErr != nil {
					return fmt.Errorf("commit invalid kafka message offset=%d: %w", message.Offset, commitErr)
				}
				continue
			}

			return fmt.Errorf("handle kafka message offset=%d: %w", message.Offset, err)
		}

		if err := c.reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit kafka message offset=%d: %w", message.Offset, err)
		}
	}
}

func (c *RideCancelledConsumer) handleMessage(ctx context.Context, message kafkago.Message) error {
	var event events.RideCancelledV1
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return &invalidMessageError{err: fmt.Errorf("decode ride cancelled event: %w", err)}
	}

	if event.CancelledBy != schemamodels.CancelledByDriver || event.Stage != "assigned" {
		log.Printf("processed ride.cancelled.v1 topic=%s partition=%d offset=%d request_id=%s (skipped: cancelled_by=%s stage=%s)", message.Topic, message.Partition, message.Offset, event.RequestID, event.CancelledBy, event.Stage)
		return nil
	}

	requestID, err := uuid.Parse(event.RequestID)
	if err != nil {
		return &invalidMessageError{err: fmt.Errorf("parse request_id %q: %w", event.RequestID, err)}
	}

	if err := c.service.HandleDriverCancellation(ctx, requestID); err != nil {
		if errors.Is(err, dispatch.ErrRequestNotFound) {
			return &invalidMessageError{err: err}
		}
		return fmt.Errorf("handle driver cancellation request_id=%s: %w", requestID, err)
	}

	log.Printf("processed ride.cancelled.v1 topic=%s partition=%d offset=%d request_id=%s", message.Topic, message.Partition, message.Offset, requestID)
	return nil
}
