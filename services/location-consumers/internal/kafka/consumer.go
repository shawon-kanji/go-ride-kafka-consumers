package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"go-ride-kafka-consumers/services/location-consumers/internal/config"
	"go-ride-kafka-consumers/services/location-consumers/internal/db/models"
	"go-ride-kafka-consumers/services/location-consumers/internal/domain/location"
	"go-ride-kafka-consumers/services/location-consumers/pkg/events"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Consumer interface {
	Start(ctx context.Context) error
}

type DriverLocationConsumer struct {
	reader *kafkago.Reader
	db     *gorm.DB
	topic  string
	group  string
}

func NewDriverLocationConsumer(cfg config.Config, db *gorm.DB) *DriverLocationConsumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.KafkaTopic,
		GroupID:  cfg.ConsumerGroup,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &DriverLocationConsumer{
		reader: reader,
		db:     db,
		topic:  cfg.KafkaTopic,
		group:  cfg.ConsumerGroup,
	}
}

func (c *DriverLocationConsumer) Start(ctx context.Context) error {
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
				log.Printf("skip invalid location message topic=%s partition=%d offset=%d: %v", message.Topic, message.Partition, message.Offset, err)
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

func (c *DriverLocationConsumer) handleMessage(ctx context.Context, message kafkago.Message) error {
	var event events.DriverLocationUpdatedV1
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return &invalidMessageError{err: fmt.Errorf("decode driver location event: %w", err)}
	}

	driverID, err := uuid.Parse(event.DriverID)
	if err != nil {
		return &invalidMessageError{err: fmt.Errorf("parse driver_id %q: %w", event.DriverID, err)}
	}

	recordedAt := event.EventTime
	if recordedAt.IsZero() {
		recordedAt = event.PublishedAt
	}
	if recordedAt.IsZero() {
		recordedAt = time.Now().UTC()
	}

	now := time.Now().UTC()
	entity := &location.DriverLocation{
		ID:         uuid.New(),
		DriverID:   driverID,
		Latitude:   event.Latitude,
		Longitude:  event.Longitude,
		Geohash:    event.Geohash,
		S2CellID:   event.S2CellID,
		RecordedAt: recordedAt,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	model := models.FromDomainDriverLocation(entity)
	if err := c.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "driver_id"}},
		DoUpdates: clause.Assignments(map[string]any{
			"latitude":    model.Latitude,
			"longitude":   model.Longitude,
			"geohash":     model.Geohash,
			"s2_cell_id":  model.S2CellID,
			"recorded_at": model.RecordedAt,
			"updated_at":  now,
		}),
	}).Create(model).Error; err != nil {
		return fmt.Errorf("upsert driver location driver_id=%s: %w", driverID, err)
	}

	log.Printf("processed driver location topic=%s partition=%d offset=%d driver_id=%s", message.Topic, message.Partition, message.Offset, driverID)
	return nil
}
