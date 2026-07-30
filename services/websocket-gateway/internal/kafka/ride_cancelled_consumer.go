package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/config"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/tripcancel"

	"github.com/shawon-kanji/go-ride-utils/events"

	kafkago "github.com/segmentio/kafka-go"
)

// RideCancelledConsumer follows the same FetchMessage/invalidMessageError/
// CommitMessages pattern as RideStartedConsumer. Normal per-message logging
// is fine here -- one event per cancellation, not a firehose.
type RideCancelledConsumer struct {
	reader   *kafkago.Reader
	notifier *tripcancel.Notifier
	topic    string
	group    string
}

func NewRideCancelledConsumer(cfg config.Config, notifier *tripcancel.Notifier) *RideCancelledConsumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.RideCancelledTopic,
		GroupID:  cfg.KafkaConsumerGroup,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &RideCancelledConsumer{
		reader:   reader,
		notifier: notifier,
		topic:    cfg.RideCancelledTopic,
		group:    cfg.KafkaConsumerGroup,
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

	if err := c.notifier.HandleRideCancelledEvent(ctx, event); err != nil {
		return fmt.Errorf("handle ride cancelled event request_id=%s: %w", event.RequestID, err)
	}

	log.Printf("processed ride cancelled event topic=%s partition=%d offset=%d request_id=%s stage=%s", message.Topic, message.Partition, message.Offset, event.RequestID, event.Stage)
	return nil
}
