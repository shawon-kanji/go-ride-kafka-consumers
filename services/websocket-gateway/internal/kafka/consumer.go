package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/config"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/offers"
	"go-ride-kafka-consumers/services/websocket-gateway/pkg/events"

	kafkago "github.com/segmentio/kafka-go"
)

type Consumer interface {
	Start(ctx context.Context) error
}

// OfferConsumer follows the same FetchMessage/invalidMessageError/
// CommitMessages pattern already used by trip-dispatch-worker's
// DispatchConsumer and location-consumers.
type OfferConsumer struct {
	reader   *kafkago.Reader
	notifier *offers.Notifier
	topic    string
	group    string
}

func NewOfferConsumer(cfg config.Config, notifier *offers.Notifier) *OfferConsumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.OfferCreatedTopic,
		GroupID:  cfg.KafkaConsumerGroup,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &OfferConsumer{
		reader:   reader,
		notifier: notifier,
		topic:    cfg.OfferCreatedTopic,
		group:    cfg.KafkaConsumerGroup,
	}
}

func (c *OfferConsumer) Start(ctx context.Context) error {
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
				log.Printf("skip invalid job offer message topic=%s partition=%d offset=%d: %v", message.Topic, message.Partition, message.Offset, err)
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

func (c *OfferConsumer) handleMessage(ctx context.Context, message kafkago.Message) error {
	var event events.JobOfferV1
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return &invalidMessageError{err: fmt.Errorf("decode job offer event: %w", err)}
	}

	if err := c.notifier.HandleJobOfferEvent(ctx, event); err != nil {
		return fmt.Errorf("handle job offer event request_id=%s: %w", event.RequestID, err)
	}

	log.Printf("processed job offer event topic=%s partition=%d offset=%d request_id=%s offer_count=%d", message.Topic, message.Partition, message.Offset, event.RequestID, len(event.Offers))
	return nil
}
