package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/shawon-kanji/go-ride-utils/events"
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

// driverCancelledEventType matches the trip_history.event_type value written
// by driver-request-handler's CancelTrip when a driver cancels a trip they'd
// accepted. Used to permanently exclude that driver from being rematched to
// the same request during redispatch.
const driverCancelledEventType = "driver_cancelled"

const nearestDriversQueryTemplate = `
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
      %s
)
SELECT cd.driver_id, cd.distance_km
FROM candidate_distances cd
WHERE cd.distance_km <= ?
  AND NOT EXISTS (
      SELECT 1 FROM ongoing_trips ot
      WHERE ot.driver_id = cd.driver_id
        AND ot.status IN (?)
  )
  AND NOT EXISTS (
      SELECT 1 FROM trip_history th
      WHERE th.request_id = ?
        AND th.driver_id = cd.driver_id
        AND th.event_type = ?
  )
ORDER BY cd.distance_km ASC
LIMIT ?
`

// buildNearestDriversQuery returns the query text and the s2_cell_id
// BETWEEN args (interleaved min, max per covering cell) to prepend before the
// distance_km/status/limit args. The S2 covering pre-filters driver_locations
// via its indexed, now-numeric s2_cell_id column before any Haversine trig is
// computed; it's an over-approximation of the search disc, so the exact
// distance_km <= radius filter downstream still does the final cut. If the
// covering is empty (shouldn't happen for a valid radius, but defensive),
// the geo pre-filter is skipped and the query falls back to a full scan of
// fresh driver_locations rows.
func buildNearestDriversQuery(mins, maxs []string) (string, []any) {
	if len(mins) == 0 {
		return fmt.Sprintf(nearestDriversQueryTemplate, ""), nil
	}

	var clauses strings.Builder
	clauses.WriteString("AND (")
	args := make([]any, 0, len(mins)*2)
	for i := range mins {
		if i > 0 {
			clauses.WriteString(" OR ")
		}
		clauses.WriteString("(dl.s2_cell_id BETWEEN ? AND ?)")
		args = append(args, mins[i], maxs[i])
	}
	clauses.WriteString(")")

	return fmt.Sprintf(nearestDriversQueryTemplate, clauses.String()), args
}

type driverCandidate struct {
	DriverID   uuid.UUID `gorm:"column:driver_id"`
	DistanceKM float64   `gorm:"column:distance_km"`
}

// Producer publishes the job-offer-created event to the realtime gateway's
// input topic. Defined here (rather than importing internal/kafka) to avoid
// an import cycle, since internal/kafka's consumer already imports dispatch.
type Producer interface {
	PublishJobOffer(ctx context.Context, event events.JobOfferV1) error
}

type Service struct {
	db       *gorm.DB
	cfg      config.Config
	producer Producer
}

func NewService(db *gorm.DB, cfg config.Config, producer Producer) *Service {
	return &Service{db: db, cfg: cfg, producer: producer}
}

// AttemptDispatch runs exactly one dispatch attempt for the given request_id.
// It is called both from the Kafka consumer (the initial attempt, triggered by
// ride.requested.v1) and from the periodic retry sweep (subsequent attempts).
// It is safe to call more than once for the same request_id: a row lock plus a
// status guard make repeated/concurrent calls a no-op once the request has moved
// past the dispatch stage.
func (s *Service) AttemptDispatch(ctx context.Context, requestID uuid.UUID) error {
	var req schemamodels.TripRequest
	var createdOffers []schemamodels.DriverJobOffer

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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

		coveringMins, coveringMaxs := s2CoveringRanges(req.PickupLat, req.PickupLng, radius)
		query, coveringArgs := buildNearestDriversQuery(coveringMins, coveringMaxs)

		args := make([]any, 0, 10+len(coveringArgs))
		args = append(args, req.PickupLat, req.PickupLng, req.PickupLat, freshSince)
		args = append(args, coveringArgs...)
		args = append(args, radius, schemamodels.ActiveOngoingTripStatuses(), req.ID, driverCancelledEventType, s.cfg.NearestDriversLimit)

		var candidates []driverCandidate
		if err := tx.Raw(query, args...).Scan(&candidates).Error; err != nil {
			return fmt.Errorf("query nearest drivers request_id=%s: %w", requestID, err)
		}

		previousStatus := req.Status
		attemptCount := req.DispatchAttemptCount + 1

		if len(candidates) > 0 {
			offers, err := s.createOffers(tx, req, candidates, previousStatus, attemptCount, radius, now)
			if err != nil {
				return err
			}
			createdOffers = offers
			return nil
		}

		if attemptCount < s.cfg.DispatchMaxAttempts {
			return s.scheduleRetry(tx, req, attemptCount, radius, now)
		}

		return s.timeout(tx, req, previousStatus, attemptCount, radius, now)
	})
	if err != nil {
		return err
	}

	if len(createdOffers) == 0 {
		return nil
	}

	var fare *schemamodels.TripFare
	if req.FareID != nil {
		var f schemamodels.TripFare
		err := s.db.WithContext(ctx).Where("fare_id = ?", *req.FareID).Take(&f).Error
		switch {
		case err == nil:
			fare = &f
		case errors.Is(err, gorm.ErrRecordNotFound):
			log.Printf("dispatch: trip fare not found for job offer event request_id=%s fare_id=%s (earnings omitted)", req.ID, *req.FareID)
		default:
			return fmt.Errorf("load trip fare for job offer event request_id=%s: %w", req.ID, err)
		}
	}

	event := buildJobOfferEvent(req, createdOffers, fare, s.cfg.DriverCommissionRate)
	if err := s.producer.PublishJobOffer(ctx, event); err != nil {
		return fmt.Errorf("publish job offer event request_id=%s: %w", req.ID, err)
	}

	return nil
}

func (s *Service) createOffers(tx *gorm.DB, req schemamodels.TripRequest, candidates []driverCandidate, previousStatus string, attemptCount int, radius float64, now time.Time) ([]schemamodels.DriverJobOffer, error) {
	offers := make([]schemamodels.DriverJobOffer, len(candidates))
	driverIDs := make([]string, len(candidates))
	driverUUIDs := make([]uuid.UUID, len(candidates))
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
		driverUUIDs[i] = candidate.DriverID
	}

	// A candidate driver can already have a driver_job_offers row for this
	// exact request_id from an earlier dispatch attempt — most notably, the
	// driver who lost out to whoever just accepted (their offer sits
	// "expired") when a redispatch (driver-cancellation) resets the request
	// back into the pool and re-offers to the same pool of nearby drivers.
	// (request_id, driver_id) is uniquely indexed for the request's lifetime,
	// so a plain INSERT would violate that constraint; upsert onto the
	// existing row instead, resetting it to a fresh pending offer.
	if err := tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "request_id"}, {Name: "driver_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"offer_rank", "status", "delivery_status", "delivery_attempts",
			"response_reason", "correlation_id", "offered_at", "expires_at",
			"responded_at", "withdrawn_at", "updated_at",
		}),
	}).Create(&offers).Error; err != nil {
		return nil, fmt.Errorf("create driver job offers request_id=%s: %w", req.ID, err)
	}

	// Reload rather than trust offers[i].ID from the upsert: on the
	// ON CONFLICT DO UPDATE path the pre-existing row's primary key (not the
	// locally generated uuid.New() above) is what's actually persisted, and
	// that's the job_offer_id downstream consumers (the job-offer Kafka
	// event, the driver's accept call) must reference.
	if err := tx.Where("request_id = ? AND driver_id IN ?", req.ID, driverUUIDs).
		Order("offer_rank ASC").
		Find(&offers).Error; err != nil {
		return nil, fmt.Errorf("reload driver job offers request_id=%s: %w", req.ID, err)
	}

	if err := tx.Model(&schemamodels.TripRequest{}).Where("request_id = ?", req.ID).Updates(map[string]any{
		"status":                 schemamodels.TripRequestStatusOffered,
		"dispatch_attempt_count": attemptCount,
		"dispatch_radius_km":     radius,
		"next_dispatch_at":       nil,
		"updated_at":             now,
	}).Error; err != nil {
		return nil, fmt.Errorf("transition trip request to offered request_id=%s: %w", req.ID, err)
	}

	payload, err := json.Marshal(map[string]any{
		"offer_count": len(offers),
		"radius_km":   radius,
		"attempt":     attemptCount,
		"driver_ids":  driverIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal dispatch history payload request_id=%s: %w", req.ID, err)
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
		return nil, fmt.Errorf("write trip history request_id=%s: %w", req.ID, err)
	}

	log.Printf("dispatch offers created request_id=%s trip_id=%s offer_count=%d radius_km=%.1f attempt=%d", req.ID, req.TripID, len(offers), radius, attemptCount)

	return offers, nil
}

// buildJobOfferEvent assembles the batch event published to the realtime
// gateway after the dispatch transaction has committed. Pickup/dropoff are
// denormalized here so the gateway's live-push path never needs a
// trip_requests join (only its low-frequency reconnect-replay path does).
func buildJobOfferEvent(req schemamodels.TripRequest, offers []schemamodels.DriverJobOffer, fare *schemamodels.TripFare, commissionRate float64) events.JobOfferV1 {
	entries := make([]events.JobOfferEntry, len(offers))
	for i, offer := range offers {
		entries[i] = events.JobOfferEntry{
			JobOfferID: offer.ID.String(),
			DriverID:   offer.DriverID.String(),
			OfferRank:  offer.OfferRank,
			OfferedAt:  offer.OfferedAt,
			ExpiresAt:  offer.ExpiresAt,
		}
	}

	correlationID := ""
	if req.CorrelationID != nil {
		correlationID = *req.CorrelationID
	}

	var estimatedEarning float64
	var currencyCode string
	if fare != nil {
		estimatedEarning = fare.TotalFare * (1 - commissionRate)
		currencyCode = fare.CurrencyCode
	}

	return events.JobOfferV1{
		RequestID:        req.ID.String(),
		TripID:           req.TripID.String(),
		RiderID:          req.RiderID.String(),
		PickupLat:        req.PickupLat,
		PickupLng:        req.PickupLng,
		DropoffLat:       req.DropoffLat,
		DropoffLng:       req.DropoffLng,
		Offers:           entries,
		EstimatedEarning: estimatedEarning,
		CurrencyCode:     currencyCode,
		CorrelationID:    correlationID,
		EventID:          uuid.NewString(),
		PublishedAt:      time.Now().UTC(),
	}
}

// HandleDriverCancellation reacts to a driver cancelling a trip they'd
// already accepted, before trip start (stage "assigned"/"driver_arriving").
// It resets the request back into the dispatch pool and immediately re-runs
// AttemptDispatch, so the rider isn't left dangling waiting for the next
// sweep tick. Guarded on the DB's own state (not just trusting the event)
// so a stale/duplicate/redelivered message is a safe no-op: only a request
// that is actually TripRequestStatusCancelled with CancelledBy == driver
// gets reset.
//
// next_dispatch_at is set to now (not nil) as part of that reset: the reset
// transaction commits independently of the immediate AttemptDispatch call
// below, so if that call fails for any reason (including one it can't itself
// recover from), the request must not be left permanently stranded in
// "searching" with nothing left to pick it up — the periodic sweep, which
// only scans rows with next_dispatch_at due, is the safety net.
func (s *Service) HandleDriverCancellation(ctx context.Context, requestID uuid.UUID) error {
	var triggered bool

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var req schemamodels.TripRequest
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("request_id = ?", requestID).
			Take(&req).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: request_id=%s", ErrRequestNotFound, requestID)
		}
		if err != nil {
			return fmt.Errorf("lock trip request request_id=%s: %w", requestID, err)
		}

		if req.Status != schemamodels.TripRequestStatusCancelled || req.CancelledBy == nil || *req.CancelledBy != schemamodels.CancelledByDriver {
			log.Printf("redispatch skipped request_id=%s status=%s (not a driver-cancelled request)", req.ID, req.Status)
			return nil
		}

		now := time.Now().UTC()
		if err := tx.Model(&schemamodels.TripRequest{}).
			Where("request_id = ?", req.ID).
			Updates(map[string]any{
				"status":                 schemamodels.TripRequestStatusSearching,
				"dispatch_attempt_count": 0,
				"dispatch_radius_km":     nil,
				"next_dispatch_at":       now,
				"cancelled_at":           nil,
				"cancellation_reason":    nil,
				"cancelled_by":           nil,
				"updated_at":             now,
			}).Error; err != nil {
			return fmt.Errorf("reset trip request for redispatch request_id=%s: %w", req.ID, err)
		}

		payload, err := json.Marshal(map[string]any{"reason": "driver_cancelled"})
		if err != nil {
			return fmt.Errorf("marshal dispatch history payload request_id=%s: %w", req.ID, err)
		}
		fromStatus := schemamodels.TripRequestStatusCancelled
		toStatus := schemamodels.TripRequestStatusSearching
		history := schemamodels.TripHistory{
			ID:            uuid.New(),
			RequestID:     req.ID,
			TripID:        req.TripID,
			RiderID:       &req.RiderID,
			EventType:     "dispatch_redispatch_triggered",
			FromStatus:    &fromStatus,
			ToStatus:      &toStatus,
			EventPayload:  payload,
			CorrelationID: req.CorrelationID,
			CreatedAt:     now,
		}
		if err := tx.Create(&history).Error; err != nil {
			return fmt.Errorf("write trip history request_id=%s: %w", req.ID, err)
		}

		triggered = true
		return nil
	})
	if err != nil {
		return err
	}
	if !triggered {
		return nil
	}

	log.Printf("redispatch triggered request_id=%s", requestID)
	return s.AttemptDispatch(ctx, requestID)
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
