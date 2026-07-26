package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/assignments"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/config"
	"github.com/shawon-kanji/go-ride-utils/events"

	kafkago "github.com/segmentio/kafka-go"
)

// AssignmentConsumer follows the same FetchMessage/invalidMessageError/
// CommitMessages pattern as OfferConsumer.
type AssignmentConsumer struct {
	reader   *kafkago.Reader
	notifier *assignments.Notifier
	topic    string
	group    string
}

func NewAssignmentConsumer(cfg config.Config, notifier *assignments.Notifier) *AssignmentConsumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.RideAssignedTopic,
		GroupID:  cfg.KafkaConsumerGroup,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &AssignmentConsumer{
		reader:   reader,
		notifier: notifier,
		topic:    cfg.RideAssignedTopic,
		group:    cfg.KafkaConsumerGroup,
	}
}

func (c *AssignmentConsumer) Start(ctx context.Context) error {
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
				log.Printf("skip invalid ride assigned message topic=%s partition=%d offset=%d: %v", message.Topic, message.Partition, message.Offset, err)
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

func (c *AssignmentConsumer) handleMessage(ctx context.Context, message kafkago.Message) error {
	var event events.RideAssignedV1
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return &invalidMessageError{err: fmt.Errorf("decode ride assigned event: %w", err)}
	}

	if err := c.notifier.HandleRideAssignedEvent(ctx, event); err != nil {
		return fmt.Errorf("handle ride assigned event request_id=%s: %w", event.RequestID, err)
	}

	log.Printf("processed ride assigned event topic=%s partition=%d offset=%d request_id=%s", message.Topic, message.Partition, message.Offset, event.RequestID)
	return nil
}
