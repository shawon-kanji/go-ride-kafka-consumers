package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/config"

	"github.com/google/uuid"
	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrRequestNotFound indicates the given request_id has no matching trip_requests
// row. Callers (e.g. the Kafka consumer) should treat this as a poison-pill/invalid
// message rather than retrying indefinitely.
var ErrRequestNotFound = errors.New("trip request not found")

const nearestDriversQuery = `
WITH candidate_distances AS (
    SELECT
        dl.driver_id,
        (
            6371 * acos(
                LEAST(1, GREATEST(-1,
                    cos(radians(?)) * cos(radians(dl.latitude)) *
                    cos(radians(dl.longitude) - radians(?)) +
                    sin(radians(?)) * sin(radians(dl.latitude))
                ))
            )
        ) AS distance_km
    FROM driver_locations dl
    WHERE dl.recorded_at >= ?
)
SELECT cd.driver_id, cd.distance_km
FROM candidate_distances cd
WHERE cd.distance_km <= ?
  AND NOT EXISTS (
      SELECT 1 FROM ongoing_trips ot
      WHERE ot.driver_id = cd.driver_id
        AND ot.status IN (?)
  )
ORDER BY cd.distance_km ASC
LIMIT ?
`

type driverCandidate struct {
	DriverID   uuid.UUID `gorm:"column:driver_id"`
	DistanceKM float64   `gorm:"column:distance_km"`
}

type Service struct {
	db  *gorm.DB
	cfg config.Config
}

func NewService(db *gorm.DB, cfg config.Config) *Service {
	return &Service{db: db, cfg: cfg}
}

// AttemptDispatch runs exactly one dispatch attempt for the given request_id.
// It is called both from the Kafka consumer (the initial attempt, triggered by
// ride.requested.v1) and from the periodic retry sweep (subsequent attempts).
// It is safe to call more than once for the same request_id: a row lock plus a
// status guard make repeated/concurrent calls a no-op once the request has moved
// past the dispatch stage.
func (s *Service) AttemptDispatch(ctx context.Context, requestID uuid.UUID) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var req schemamodels.TripRequest
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("request_id = ?", requestID).
			Take(&req).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: request_id=%s", ErrRequestNotFound, requestID)
		}
		if err != nil {
			return fmt.Errorf("load trip request request_id=%s: %w", requestID, err)
		}

		if req.Status != schemamodels.TripRequestStatusSearchStarted && req.Status != schemamodels.TripRequestStatusSearching {
			log.Printf("dispatch attempt skipped request_id=%s status=%s (already past dispatch stage)", req.ID, req.Status)
			return nil
		}

		radius := req.SearchRadiusKM
		if req.DispatchRadiusKM != nil {
			radius = *req.DispatchRadiusKM
		}
		if radius <= 0 {
			radius = s.cfg.DispatchInitialRadiusKM
		}

		now := time.Now().UTC()
		freshSince := now.Add(-s.cfg.DriverLocationFreshWindow)

		var candidates []driverCandidate
		if err := tx.Raw(
			nearestDriversQuery,
			req.PickupLat, req.PickupLng, req.PickupLat,
			freshSince,
			radius,
			schemamodels.ActiveOngoingTripStatuses(),
			s.cfg.NearestDriversLimit,
		).Scan(&candidates).Error; err != nil {
			return fmt.Errorf("query nearest drivers request_id=%s: %w", requestID, err)
		}

		previousStatus := req.Status
		attemptCount := req.DispatchAttemptCount + 1

		if len(candidates) > 0 {
			return s.createOffers(tx, req, candidates, previousStatus, attemptCount, radius, now)
		}

		if attemptCount < s.cfg.DispatchMaxAttempts {
			return s.scheduleRetry(tx, req, attemptCount, radius, now)
		}

		return s.timeout(tx, req, previousStatus, attemptCount, radius, now)
	})
}

func (s *Service) createOffers(tx *gorm.DB, req schemamodels.TripRequest, candidates []driverCandidate, previousStatus string, attemptCount int, radius float64, now time.Time) error {
	offers := make([]schemamodels.DriverJobOffer, len(candidates))
	driverIDs := make([]string, len(candidates))
	expiresAt := now.Add(time.Duration(s.cfg.JobOfferTTLSeconds) * time.Second)

	for i, candidate := range candidates {
		offers[i] = schemamodels.DriverJobOffer{
			ID:             uuid.New(),
			RequestID:      req.ID,
			TripID:         req.TripID,
			DriverID:       candidate.DriverID,
			OfferRank:      i,
			Status:         schemamodels.DriverJobOfferStatusPending,
			DeliveryStatus: schemamodels.DriverJobOfferDeliveryStatusPending,
			CorrelationID:  req.CorrelationID,
			OfferedAt:      now,
			ExpiresAt:      expiresAt,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		driverIDs[i] = candidate.DriverID.String()
	}

	if err := tx.Create(&offers).Error; err != nil {
		return fmt.Errorf("create driver job offers request_id=%s: %w", req.ID, err)
	}

	if err := tx.Model(&schemamodels.TripRequest{}).Where("request_id = ?", req.ID).Updates(map[string]any{
		"status":                 schemamodels.TripRequestStatusOffered,
		"dispatch_attempt_count": attemptCount,
		"dispatch_radius_km":     radius,
		"next_dispatch_at":       nil,
		"updated_at":             now,
	}).Error; err != nil {
		return fmt.Errorf("transition trip request to offered request_id=%s: %w", req.ID, err)
	}

	payload, err := json.Marshal(map[string]any{
		"offer_count": len(offers),
		"radius_km":   radius,
		"attempt":     attemptCount,
		"driver_ids":  driverIDs,
	})
	if err != nil {
		return fmt.Errorf("marshal dispatch history payload request_id=%s: %w", req.ID, err)
	}

	toStatus := schemamodels.TripRequestStatusOffered
	history := schemamodels.TripHistory{
		ID:            uuid.New(),
		RequestID:     req.ID,
		TripID:        req.TripID,
		RiderID:       &req.RiderID,
		EventType:     "dispatch_offers_created",
		FromStatus:    stringPtr(previousStatus),
		ToStatus:      &toStatus,
		EventPayload:  payload,
		CorrelationID: req.CorrelationID,
		CreatedAt:     now,
	}
	if err := tx.Create(&history).Error; err != nil {
		return fmt.Errorf("write trip history request_id=%s: %w", req.ID, err)
	}

	log.Printf("dispatch offers created request_id=%s trip_id=%s offer_count=%d radius_km=%.1f attempt=%d", req.ID, req.TripID, len(offers), radius, attemptCount)
	return nil
}

func (s *Service) scheduleRetry(tx *gorm.DB, req schemamodels.TripRequest, attemptCount int, radius float64, now time.Time) error {
	nextRadius := min(radius+s.cfg.DispatchRadiusStepKM, s.cfg.DispatchMaxRadiusKM)
	backoffSeconds := min(s.cfg.DispatchBackoffBaseSecs<<(attemptCount-1), s.cfg.DispatchBackoffMaxSecs)
	nextDispatchAt := now.Add(time.Duration(backoffSeconds) * time.Second)

	if err := tx.Model(&schemamodels.TripRequest{}).Where("request_id = ?", req.ID).Updates(map[string]any{
		"status":                 schemamodels.TripRequestStatusSearching,
		"dispatch_attempt_count": attemptCount,
		"dispatch_radius_km":     nextRadius,
		"next_dispatch_at":       nextDispatchAt,
		"updated_at":             now,
	}).Error; err != nil {
		return fmt.Errorf("schedule dispatch retry request_id=%s: %w", req.ID, err)
	}

	log.Printf("dispatch retry scheduled request_id=%s attempt=%d next_radius_km=%.1f next_dispatch_at=%s", req.ID, attemptCount, nextRadius, nextDispatchAt.Format(time.RFC3339))
	return nil
}

func (s *Service) timeout(tx *gorm.DB, req schemamodels.TripRequest, previousStatus string, attemptCount int, radius float64, now time.Time) error {
	if err := tx.Model(&schemamodels.TripRequest{}).Where("request_id = ?", req.ID).Updates(map[string]any{
		"status":                 schemamodels.TripRequestStatusTimedOut,
		"dispatch_attempt_count": attemptCount,
		"next_dispatch_at":       nil,
		"timed_out_at":           now,
		"updated_at":             now,
	}).Error; err != nil {
		return fmt.Errorf("transition trip request to timed_out request_id=%s: %w", req.ID, err)
	}

	payload, err := json.Marshal(map[string]any{
		"attempts":        attemptCount,
		"final_radius_km": radius,
	})
	if err != nil {
		return fmt.Errorf("marshal dispatch history payload request_id=%s: %w", req.ID, err)
	}

	toStatus := schemamodels.TripRequestStatusTimedOut
	history := schemamodels.TripHistory{
		ID:            uuid.New(),
		RequestID:     req.ID,
		TripID:        req.TripID,
		RiderID:       &req.RiderID,
		EventType:     "dispatch_timed_out",
		FromStatus:    stringPtr(previousStatus),
		ToStatus:      &toStatus,
		EventPayload:  payload,
		CorrelationID: req.CorrelationID,
		CreatedAt:     now,
	}
	if err := tx.Create(&history).Error; err != nil {
		return fmt.Errorf("write trip history request_id=%s: %w", req.ID, err)
	}

	log.Printf("dispatch timed out request_id=%s trip_id=%s attempts=%d final_radius_km=%.1f", req.ID, req.TripID, attemptCount, radius)
	return nil
}

func stringPtr(value string) *string {
	return &value
}
