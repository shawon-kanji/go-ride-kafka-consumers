package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/config"
	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/dispatch"
	"go-ride-kafka-consumers/services/trip-dispatch-worker/pkg/events"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

type Consumer interface {
	Start(ctx context.Context) error
}

type DispatchConsumer struct {
	reader  *kafkago.Reader
	service *dispatch.Service
	topic   string
	group   string
}

func NewDispatchConsumer(cfg config.Config, service *dispatch.Service) *DispatchConsumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.InputTopic,
		GroupID:  cfg.ConsumerGroup,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &DispatchConsumer{
		reader:  reader,
		service: service,
		topic:   cfg.InputTopic,
		group:   cfg.ConsumerGroup,
	}
}

func (c *DispatchConsumer) Start(ctx context.Context) error {
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
				log.Printf("skip invalid ride requested message topic=%s partition=%d offset=%d: %v", message.Topic, message.Partition, message.Offset, err)
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

type invalidMessageError struct {
	err error
}

func (e *invalidMessageError) Error() string {
	return e.err.Error()
}

func (e *invalidMessageError) Unwrap() error {
	return e.err
}

func (c *DispatchConsumer) handleMessage(ctx context.Context, message kafkago.Message) error {
	var event events.RideRequestedV1
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return &invalidMessageError{err: fmt.Errorf("decode ride requested event: %w", err)}
	}

	requestID, err := uuid.Parse(event.RequestID)
	if err != nil {
		return &invalidMessageError{err: fmt.Errorf("parse request_id %q: %w", event.RequestID, err)}
	}

	if err := c.service.AttemptDispatch(ctx, requestID); err != nil {
		if errors.Is(err, dispatch.ErrRequestNotFound) {
			return &invalidMessageError{err: err}
		}
		return fmt.Errorf("attempt dispatch request_id=%s: %w", requestID, err)
	}

	log.Printf("processed ride.requested.v1 topic=%s partition=%d offset=%d request_id=%s", message.Topic, message.Partition, message.Offset, requestID)
	return nil
}
