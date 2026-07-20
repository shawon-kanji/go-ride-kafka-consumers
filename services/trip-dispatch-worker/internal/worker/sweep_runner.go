package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/dispatch"

	"github.com/google/uuid"
	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
	"gorm.io/gorm"
)

// SweepRunner periodically picks up trip_requests whose next_dispatch_at has
// arrived (retry attempts scheduled by dispatch.Service after a zero-driver
// attempt) and re-runs the dispatch attempt for each. Unlike the Kafka consumer,
// there is no offset/redelivery mechanism to lean on here, so failures are
// logged and swallowed per-item and per-tick rather than propagated — a
// transient DB blip on one sweep must not take down the whole service (and, in
// particular, must not cancel the sibling Kafka consumer goroutine).
type SweepRunner struct {
	db          *gorm.DB
	service     *dispatch.Service
	interval    time.Duration
	maxAttempts int
}

func NewSweepRunner(db *gorm.DB, service *dispatch.Service, interval time.Duration, maxAttempts int) *SweepRunner {
	return &SweepRunner{db: db, service: service, interval: interval, maxAttempts: maxAttempts}
}

func (r *SweepRunner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	log.Printf("dispatch sweep started interval=%s max_attempts=%d", r.interval, r.maxAttempts)

	for {
		select {
		case <-ctx.Done():
			log.Printf("dispatch sweep stopping")
			return nil
		case <-ticker.C:
			if err := r.sweepOnce(ctx); err != nil {
				log.Printf("dispatch sweep error: %v", err)
			}
		}
	}
}

func (r *SweepRunner) sweepOnce(ctx context.Context) error {
	var requestIDs []uuid.UUID
	err := r.db.WithContext(ctx).
		Model(&schemamodels.TripRequest{}).
		Where("status = ? AND next_dispatch_at IS NOT NULL AND next_dispatch_at <= ? AND dispatch_attempt_count < ?",
			schemamodels.TripRequestStatusSearching, time.Now().UTC(), r.maxAttempts).
		Pluck("request_id", &requestIDs).Error
	if err != nil {
		return fmt.Errorf("query sweep-eligible requests: %w", err)
	}

	for _, id := range requestIDs {
		if err := r.service.AttemptDispatch(ctx, id); err != nil {
			log.Printf("sweep dispatch attempt failed request_id=%s: %v", id, err)
		}
	}

	return nil
}
