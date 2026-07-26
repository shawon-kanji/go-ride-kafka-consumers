package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/config"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/tripcomplete"
	"go-ride-kafka-consumers/services/websocket-gateway/pkg/events"

	kafkago "github.com/segmentio/kafka-go"
)

// RideCompletedConsumer follows the same FetchMessage/invalidMessageError/
// CommitMessages pattern as RideStartedConsumer. Normal per-message logging
// is fine here — one event per trip completion, not a firehose.
type RideCompletedConsumer struct {
	reader   *kafkago.Reader
	notifier *tripcomplete.Notifier
	topic    string
	group    string
}

func NewRideCompletedConsumer(cfg config.Config, notifier *tripcomplete.Notifier) *RideCompletedConsumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.RideCompletedTopic,
		GroupID:  cfg.KafkaConsumerGroup,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &RideCompletedConsumer{
		reader:   reader,
		notifier: notifier,
		topic:    cfg.RideCompletedTopic,
		group:    cfg.KafkaConsumerGroup,
	}
}

func (c *RideCompletedConsumer) Start(ctx context.Context) error {
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
				log.Printf("skip invalid ride completed message topic=%s partition=%d offset=%d: %v", message.Topic, message.Partition, message.Offset, err)
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

func (c *RideCompletedConsumer) handleMessage(ctx context.Context, message kafkago.Message) error {
	var event events.RideCompletedV1
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return &invalidMessageError{err: fmt.Errorf("decode ride completed event: %w", err)}
	}

	if err := c.notifier.HandleRideCompletedEvent(ctx, event); err != nil {
		return fmt.Errorf("handle ride completed event request_id=%s: %w", event.RequestID, err)
	}

	log.Printf("processed ride completed event topic=%s partition=%d offset=%d request_id=%s", message.Topic, message.Partition, message.Offset, event.RequestID)
	return nil
}
