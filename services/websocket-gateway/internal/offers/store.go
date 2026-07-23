package offers

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
	"gorm.io/gorm"
)

// Store performs the gateway's writes to driver_job_offers. It is scoped
// strictly to delivery-tracking columns (delivery_status, delivery_attempts,
// responded_at) — the status column (pending/accepted/rejected/expired/
// withdrawn) stays exclusively owned by trip-dispatch-worker, both today and
// once Phase 7's accept/reject logic lands there.
type Store struct {
	db *gorm.DB
}

func NewStore(db *gorm.DB) *Store {
	return &Store{db: db}
}

// MarkSent records that an offer was successfully pushed over the websocket
// (live push or replay).
func (s *Store) MarkSent(ctx context.Context, jobOfferID uuid.UUID) error {
	now := time.Now().UTC()
	err := s.db.WithContext(ctx).Model(&schemamodels.DriverJobOffer{}).
		Where("job_offer_id = ?", jobOfferID).
		Updates(map[string]any{
			"delivery_status":   schemamodels.DriverJobOfferDeliveryStatusSent,
			"delivery_attempts": gorm.Expr("delivery_attempts + 1"),
			"updated_at":        now,
		}).Error
	if err != nil {
		return fmt.Errorf("mark job offer sent job_offer_id=%s: %w", jobOfferID, err)
	}
	return nil
}

// MarkAck records a driver's ack frame. "delivered"/"seen" map onto
// delivery_status; "accepted"/"rejected" are recorded as responded_at only —
// Phase 6 does not implement the assignment side effects (first-wins lock,
// trip_requests/ongoing_trips updates, ride.assigned.v1 publish) that a real
// accept/reject requires. That's Phase 7.
func (s *Store) MarkAck(ctx context.Context, jobOfferID uuid.UUID, ackStatus string) error {
	now := time.Now().UTC()
	updates := map[string]any{
		"responded_at": now,
		"updated_at":   now,
	}

	switch ackStatus {
	case "delivered":
		updates["delivery_status"] = schemamodels.DriverJobOfferDeliveryStatusDelivered
	case "seen":
		updates["delivery_status"] = schemamodels.DriverJobOfferDeliveryStatusSeen
	}

	err := s.db.WithContext(ctx).Model(&schemamodels.DriverJobOffer{}).
		Where("job_offer_id = ?", jobOfferID).
		Updates(updates).Error
	if err != nil {
		return fmt.Errorf("mark job offer ack job_offer_id=%s status=%s: %w", jobOfferID, ackStatus, err)
	}
	return nil
}
