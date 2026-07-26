package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/config"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/tracking"
	"github.com/shawon-kanji/go-ride-utils/events"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

// LocationConsumer follows the same FetchMessage/invalidMessageError/
// CommitMessages pattern as OfferConsumer/AssignmentConsumer, with one
// deliberate deviation: it does not log a line per successfully processed
// message. driver.location.updated.v1 is a fleet-wide, continuous firehose
// (unlike the low-volume offer/assignment topics), so per-message logging
// here would spam badly. Only the rare invalid-message-skip case still logs.
type LocationConsumer struct {
	reader   *kafkago.Reader
	notifier *tracking.Notifier
	topic    string
	group    string
}

func NewLocationConsumer(cfg config.Config, notifier *tracking.Notifier) *LocationConsumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.DriverLocationTopic,
		GroupID:  cfg.KafkaConsumerGroup,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &LocationConsumer{
		reader:   reader,
		notifier: notifier,
		topic:    cfg.DriverLocationTopic,
		group:    cfg.KafkaConsumerGroup,
	}
}

func (c *LocationConsumer) Start(ctx context.Context) error {
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
				log.Printf("skip invalid driver location message topic=%s partition=%d offset=%d: %v", message.Topic, message.Partition, message.Offset, err)
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

func (c *LocationConsumer) handleMessage(ctx context.Context, message kafkago.Message) error {
	var event events.DriverLocationUpdatedV1
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return &invalidMessageError{err: fmt.Errorf("decode driver location event: %w", err)}
	}

	driverID, err := uuid.Parse(event.DriverID)
	if err != nil {
		return &invalidMessageError{err: fmt.Errorf("invalid driver_id=%q: %w", event.DriverID, err)}
	}
	event.DriverID = driverID.String()

	if err := c.notifier.HandleDriverLocationUpdatedEvent(ctx, event); err != nil {
		return fmt.Errorf("handle driver location event driver_id=%s: %w", event.DriverID, err)
	}

	return nil
}
