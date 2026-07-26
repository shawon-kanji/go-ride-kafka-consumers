package offers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go-ride-kafka-consumers/services/driver-request-handler/internal/kafka"
	"go-ride-kafka-consumers/services/driver-request-handler/pkg/events"

	"github.com/google/uuid"
	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrOfferNotFound    = errors.New("offer_not_found")
	ErrOfferForbidden   = errors.New("offer_forbidden")
	ErrOfferNotWinnable = errors.New("offer_not_winnable")

	ErrTripNotFound       = errors.New("trip_not_found")
	ErrTripForbidden      = errors.New("trip_forbidden")
	ErrTripNotStartable   = errors.New("trip_not_startable")
	ErrTripNotEndable     = errors.New("trip_not_endable")
	ErrTripNotCollectable = errors.New("trip_not_collectable")
)

type AcceptResult struct {
	TripRequest schemamodels.TripRequest
	OngoingTrip schemamodels.OngoingTrip
	Fare        *schemamodels.TripFare
}

type StartTripResult struct {
	OngoingTrip schemamodels.OngoingTrip
}

type EndTripResult struct {
	OngoingTrip  schemamodels.OngoingTrip
	CurrencyCode string
}

type CollectPaymentResult struct {
	OngoingTrip  schemamodels.OngoingTrip
	CurrencyCode string
}

type Service struct {
	db       *gorm.DB
	producer kafka.Producer
}

func NewService(db *gorm.DB, producer kafka.Producer) *Service {
	return &Service{db: db, producer: producer}
}

// AcceptOffer implements the first-wins acceptance lock: it row-locks the
// job offer and its parent trip request, verifies the offer still belongs to
// driverID and is live, and only then flips status across
// driver_job_offers/trip_requests/ongoing_trips/trip_history atomically.
// Callers that lose the race (trip_requests.status != offered) get
// ErrOfferNotWinnable.
func (s *Service) AcceptOffer(ctx context.Context, jobOfferID, driverID uuid.UUID) (AcceptResult, error) {
	now := time.Now().UTC()
	var result AcceptResult

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var offer schemamodels.DriverJobOffer
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("job_offer_id = ?", jobOfferID).
			Take(&offer).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrOfferNotFound
		}
		if err != nil {
			return fmt.Errorf("lock job offer job_offer_id=%s: %w", jobOfferID, err)
		}

		if offer.DriverID != driverID {
			return ErrOfferForbidden
		}
		if offer.Status != schemamodels.DriverJobOfferStatusPending || now.After(offer.ExpiresAt) {
			return ErrOfferNotWinnable
		}

		var request schemamodels.TripRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("request_id = ?", offer.RequestID).
			Take(&request).Error; err != nil {
			return fmt.Errorf("lock trip request request_id=%s: %w", offer.RequestID, err)
		}

		if request.Status != schemamodels.TripRequestStatusOffered {
			return ErrOfferNotWinnable
		}

		if err := tx.Model(&schemamodels.DriverJobOffer{}).
			Where("job_offer_id = ?", offer.ID).
			Updates(map[string]any{"status": schemamodels.DriverJobOfferStatusAccepted, "responded_at": now, "updated_at": now}).Error; err != nil {
			return fmt.Errorf("mark offer accepted: %w", err)
		}

		if err := tx.Model(&schemamodels.DriverJobOffer{}).
			Where("request_id = ? AND job_offer_id <> ? AND status = ?", offer.RequestID, offer.ID, schemamodels.DriverJobOfferStatusPending).
			Updates(map[string]any{"status": schemamodels.DriverJobOfferStatusExpired, "updated_at": now}).Error; err != nil {
			return fmt.Errorf("expire losing offers request_id=%s: %w", offer.RequestID, err)
		}

		if err := tx.Model(&schemamodels.TripRequest{}).
			Where("request_id = ?", request.ID).
			Updates(map[string]any{"status": schemamodels.TripRequestStatusAssigned, "assigned_at": now, "updated_at": now}).Error; err != nil {
			return fmt.Errorf("mark trip request assigned: %w", err)
		}
		request.Status = schemamodels.TripRequestStatusAssigned
		request.AssignedAt = &now

		ongoingTrip := schemamodels.OngoingTrip{
			ID:         uuid.New(),
			RequestID:  request.ID,
			TripID:     request.TripID,
			RiderID:    request.RiderID,
			DriverID:   driverID,
			Status:     schemamodels.OngoingTripStatusAssigned,
			PickupLat:  request.PickupLat,
			PickupLng:  request.PickupLng,
			DropoffLat: request.DropoffLat,
			DropoffLng: request.DropoffLng,
			AssignedAt: now,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		if err := tx.Create(&ongoingTrip).Error; err != nil {
			return fmt.Errorf("create ongoing trip: %w", err)
		}

		eventPayload, err := json.Marshal(map[string]any{
			"job_offer_id":    offer.ID,
			"ongoing_trip_id": ongoingTrip.ID,
		})
		if err != nil {
			return fmt.Errorf("marshal trip history payload: %w", err)
		}
		fromStatus := schemamodels.TripRequestStatusOffered
		toStatus := schemamodels.TripRequestStatusAssigned
		history := schemamodels.TripHistory{
			ID:           uuid.New(),
			RequestID:    request.ID,
			TripID:       request.TripID,
			RiderID:      &request.RiderID,
			DriverID:     &driverID,
			EventType:    "driver_accepted",
			FromStatus:   &fromStatus,
			ToStatus:     &toStatus,
			EventPayload: eventPayload,
			CreatedAt:    now,
		}
		if request.CorrelationID != nil {
			history.CorrelationID = request.CorrelationID
		}
		if err := tx.Create(&history).Error; err != nil {
			return fmt.Errorf("create trip history: %w", err)
		}

		var fare *schemamodels.TripFare
		if request.FareID != nil {
			var loaded schemamodels.TripFare
			if err := tx.Where("fare_id = ?", *request.FareID).Take(&loaded).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("load trip fare fare_id=%s: %w", request.FareID.String(), err)
			} else if err == nil {
				fare = &loaded
			}
		}

		result = AcceptResult{TripRequest: request, OngoingTrip: ongoingTrip, Fare: fare}
		return nil
	})
	if err != nil {
		return AcceptResult{}, err
	}

	var driverLat, driverLng *float64
	var location schemamodels.DriverLocation
	if err := s.db.WithContext(ctx).Where("driver_id = ?", driverID).Take(&location).Error; err == nil {
		driverLat, driverLng = &location.Latitude, &location.Longitude
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return AcceptResult{}, fmt.Errorf("load driver location driver_id=%s: %w", driverID, err)
	}

	var driver schemamodels.Driver
	driverName := ""
	if err := s.db.WithContext(ctx).Where("id = ?", driverID).Take(&driver).Error; err == nil {
		driverName = driver.FirstName + " " + driver.LastName
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return AcceptResult{}, fmt.Errorf("load driver driver_id=%s: %w", driverID, err)
	}

	event := events.RideAssignedV1{
		RequestID:     result.TripRequest.ID.String(),
		TripID:        result.TripRequest.TripID.String(),
		OngoingTripID: result.OngoingTrip.ID.String(),
		RiderID:       result.TripRequest.RiderID.String(),
		DriverID:      driverID.String(),
		DriverName:    driverName,
		DriverLat:     driverLat,
		DriverLng:     driverLng,
		PickupLat:     result.TripRequest.PickupLat,
		PickupLng:     result.TripRequest.PickupLng,
		DropoffLat:    result.TripRequest.DropoffLat,
		DropoffLng:    result.TripRequest.DropoffLng,
		EventID:       result.OngoingTrip.ID.String(),
		PublishedAt:   time.Now().UTC(),
	}
	if result.TripRequest.CorrelationID != nil {
		event.CorrelationID = *result.TripRequest.CorrelationID
	}

	if err := s.producer.PublishRideAssigned(ctx, event); err != nil {
		return AcceptResult{}, fmt.Errorf("publish ride assigned request_id=%s: %w", event.RequestID, err)
	}

	return result, nil
}

// StartTrip marks a trip started once the driver has reached the rider and
// they're onboard: a single collapsed assigned -> in_progress transition, no
// separate "driver_arriving"/"arrived" step and no geofence validation
// against pickup coordinates — same trust-the-driver's-tap model as accept.
func (s *Service) StartTrip(ctx context.Context, ongoingTripID, driverID uuid.UUID) (StartTripResult, error) {
	now := time.Now().UTC()
	var result StartTripResult
	var correlationID *string

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var trip schemamodels.OngoingTrip
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", ongoingTripID).
			Take(&trip).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrTripNotFound
		}
		if err != nil {
			return fmt.Errorf("lock ongoing trip id=%s: %w", ongoingTripID, err)
		}

		if trip.DriverID != driverID {
			return ErrTripForbidden
		}
		if trip.Status != schemamodels.OngoingTripStatusAssigned {
			return ErrTripNotStartable
		}

		if err := tx.Model(&schemamodels.OngoingTrip{}).
			Where("id = ?", trip.ID).
			Updates(map[string]any{"status": schemamodels.OngoingTripStatusInProgress, "started_at": now, "updated_at": now}).Error; err != nil {
			return fmt.Errorf("mark ongoing trip in_progress: %w", err)
		}
		trip.Status = schemamodels.OngoingTripStatusInProgress
		trip.StartedAt = &now

		var request schemamodels.TripRequest
		if err := tx.Where("request_id = ?", trip.RequestID).Take(&request).Error; err == nil {
			correlationID = request.CorrelationID
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("load trip request request_id=%s: %w", trip.RequestID, err)
		}

		eventPayload, err := json.Marshal(map[string]any{"ongoing_trip_id": trip.ID})
		if err != nil {
			return fmt.Errorf("marshal trip history payload: %w", err)
		}
		fromStatus := schemamodels.OngoingTripStatusAssigned
		toStatus := schemamodels.OngoingTripStatusInProgress
		history := schemamodels.TripHistory{
			ID:            uuid.New(),
			RequestID:     trip.RequestID,
			TripID:        trip.TripID,
			RiderID:       &trip.RiderID,
			DriverID:      &driverID,
			EventType:     "trip_started",
			FromStatus:    &fromStatus,
			ToStatus:      &toStatus,
			EventPayload:  eventPayload,
			CorrelationID: correlationID,
			CreatedAt:     now,
		}
		if err := tx.Create(&history).Error; err != nil {
			return fmt.Errorf("create trip history: %w", err)
		}

		result = StartTripResult{OngoingTrip: trip}
		return nil
	})
	if err != nil {
		return StartTripResult{}, err
	}

	event := events.RideStartedV1{
		RequestID:     result.OngoingTrip.RequestID.String(),
		TripID:        result.OngoingTrip.TripID.String(),
		OngoingTripID: result.OngoingTrip.ID.String(),
		RiderID:       result.OngoingTrip.RiderID.String(),
		DriverID:      driverID.String(),
		StartedAt:     now,
		EventID:       result.OngoingTrip.ID.String() + ":started",
		PublishedAt:   time.Now().UTC(),
	}
	if correlationID != nil {
		event.CorrelationID = *correlationID
	}

	if err := s.producer.PublishRideStarted(ctx, event); err != nil {
		return StartTripResult{}, fmt.Errorf("publish ride started request_id=%s: %w", event.RequestID, err)
	}

	return result, nil
}

// EndTrip marks a trip ended once the driver has reached the destination:
// in_progress -> awaiting_payment. FinalFare is the already-locked
// trip_fares.total_fare copied as-is, no live recalculation from distance/
// duration (the platform doesn't track that) — same trust-the-driver's-tap
// model as accept/start-trip.
func (s *Service) EndTrip(ctx context.Context, ongoingTripID, driverID uuid.UUID) (EndTripResult, error) {
	now := time.Now().UTC()
	var result EndTripResult
	var correlationID *string

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var trip schemamodels.OngoingTrip
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", ongoingTripID).
			Take(&trip).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrTripNotFound
		}
		if err != nil {
			return fmt.Errorf("lock ongoing trip id=%s: %w", ongoingTripID, err)
		}

		if trip.DriverID != driverID {
			return ErrTripForbidden
		}
		if trip.Status != schemamodels.OngoingTripStatusInProgress {
			return ErrTripNotEndable
		}

		var request schemamodels.TripRequest
		if err := tx.Where("request_id = ?", trip.RequestID).Take(&request).Error; err == nil {
			correlationID = request.CorrelationID
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("load trip request request_id=%s: %w", trip.RequestID, err)
		}

		var finalFare float64
		var currencyCode string
		if request.FareID != nil {
			var fare schemamodels.TripFare
			if err := tx.Where("fare_id = ?", *request.FareID).Take(&fare).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("load trip fare fare_id=%s: %w", request.FareID.String(), err)
			} else if err == nil {
				finalFare = fare.TotalFare
				currencyCode = fare.CurrencyCode
			}
		}

		paymentStatus := schemamodels.OngoingTripPaymentStatusPending
		if err := tx.Model(&schemamodels.OngoingTrip{}).
			Where("id = ?", trip.ID).
			Updates(map[string]any{
				"status":         schemamodels.OngoingTripStatusAwaitingPayment,
				"ended_at":       now,
				"final_fare":     finalFare,
				"payment_status": paymentStatus,
				"updated_at":     now,
			}).Error; err != nil {
			return fmt.Errorf("mark ongoing trip awaiting_payment: %w", err)
		}
		trip.Status = schemamodels.OngoingTripStatusAwaitingPayment
		trip.EndedAt = &now
		trip.FinalFare = &finalFare
		trip.PaymentStatus = &paymentStatus

		eventPayload, err := json.Marshal(map[string]any{
			"ongoing_trip_id": trip.ID,
			"final_fare":      finalFare,
			"currency_code":   currencyCode,
		})
		if err != nil {
			return fmt.Errorf("marshal trip history payload: %w", err)
		}
		fromStatus := schemamodels.OngoingTripStatusInProgress
		toStatus := schemamodels.OngoingTripStatusAwaitingPayment
		history := schemamodels.TripHistory{
			ID:            uuid.New(),
			RequestID:     trip.RequestID,
			TripID:        trip.TripID,
			RiderID:       &trip.RiderID,
			DriverID:      &driverID,
			EventType:     "trip_ended",
			FromStatus:    &fromStatus,
			ToStatus:      &toStatus,
			EventPayload:  eventPayload,
			CorrelationID: correlationID,
			CreatedAt:     now,
		}
		if err := tx.Create(&history).Error; err != nil {
			return fmt.Errorf("create trip history: %w", err)
		}

		result = EndTripResult{OngoingTrip: trip, CurrencyCode: currencyCode}
		return nil
	})
	if err != nil {
		return EndTripResult{}, err
	}

	event := events.RideEndedV1{
		RequestID:     result.OngoingTrip.RequestID.String(),
		TripID:        result.OngoingTrip.TripID.String(),
		OngoingTripID: result.OngoingTrip.ID.String(),
		RiderID:       result.OngoingTrip.RiderID.String(),
		DriverID:      driverID.String(),
		FinalFare:     *result.OngoingTrip.FinalFare,
		CurrencyCode:  result.CurrencyCode,
		EndedAt:       now,
		EventID:       result.OngoingTrip.ID.String() + ":ended",
		PublishedAt:   time.Now().UTC(),
	}
	if correlationID != nil {
		event.CorrelationID = *correlationID
	}

	if err := s.producer.PublishRideEnded(ctx, event); err != nil {
		return EndTripResult{}, fmt.Errorf("publish ride ended request_id=%s: %w", event.RequestID, err)
	}

	return result, nil
}

// CollectPayment fully closes a trip once the driver confirms cash was
// physically collected: awaiting_payment -> completed. Manual/cash-only
// confirmation — no payment method field, no gateway integration.
func (s *Service) CollectPayment(ctx context.Context, ongoingTripID, driverID uuid.UUID) (CollectPaymentResult, error) {
	now := time.Now().UTC()
	var result CollectPaymentResult
	var correlationID *string

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var trip schemamodels.OngoingTrip
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", ongoingTripID).
			Take(&trip).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrTripNotFound
		}
		if err != nil {
			return fmt.Errorf("lock ongoing trip id=%s: %w", ongoingTripID, err)
		}

		if trip.DriverID != driverID {
			return ErrTripForbidden
		}
		if trip.Status != schemamodels.OngoingTripStatusAwaitingPayment {
			return ErrTripNotCollectable
		}

		var request schemamodels.TripRequest
		if err := tx.Where("request_id = ?", trip.RequestID).Take(&request).Error; err == nil {
			correlationID = request.CorrelationID
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("load trip request request_id=%s: %w", trip.RequestID, err)
		}

		var currencyCode string
		if request.FareID != nil {
			var fare schemamodels.TripFare
			if err := tx.Where("fare_id = ?", *request.FareID).Take(&fare).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("load trip fare fare_id=%s: %w", request.FareID.String(), err)
			} else if err == nil {
				currencyCode = fare.CurrencyCode
			}
		}

		paymentStatus := schemamodels.OngoingTripPaymentStatusCollected
		if err := tx.Model(&schemamodels.OngoingTrip{}).
			Where("id = ?", trip.ID).
			Updates(map[string]any{
				"status":               schemamodels.OngoingTripStatusCompleted,
				"completed_at":         now,
				"payment_status":       paymentStatus,
				"payment_collected_at": now,
				"updated_at":           now,
			}).Error; err != nil {
			return fmt.Errorf("mark ongoing trip completed: %w", err)
		}
		trip.Status = schemamodels.OngoingTripStatusCompleted
		trip.CompletedAt = &now
		trip.PaymentStatus = &paymentStatus
		trip.PaymentCollectedAt = &now

		eventPayload, err := json.Marshal(map[string]any{"ongoing_trip_id": trip.ID})
		if err != nil {
			return fmt.Errorf("marshal trip history payload: %w", err)
		}
		fromStatus := schemamodels.OngoingTripStatusAwaitingPayment
		toStatus := schemamodels.OngoingTripStatusCompleted
		history := schemamodels.TripHistory{
			ID:            uuid.New(),
			RequestID:     trip.RequestID,
			TripID:        trip.TripID,
			RiderID:       &trip.RiderID,
			DriverID:      &driverID,
			EventType:     "payment_collected",
			FromStatus:    &fromStatus,
			ToStatus:      &toStatus,
			EventPayload:  eventPayload,
			CorrelationID: correlationID,
			CreatedAt:     now,
		}
		if err := tx.Create(&history).Error; err != nil {
			return fmt.Errorf("create trip history: %w", err)
		}

		result = CollectPaymentResult{OngoingTrip: trip, CurrencyCode: currencyCode}
		return nil
	})
	if err != nil {
		return CollectPaymentResult{}, err
	}

	event := events.RideCompletedV1{
		RequestID:          result.OngoingTrip.RequestID.String(),
		TripID:             result.OngoingTrip.TripID.String(),
		OngoingTripID:      result.OngoingTrip.ID.String(),
		RiderID:            result.OngoingTrip.RiderID.String(),
		DriverID:           driverID.String(),
		FinalFare:          *result.OngoingTrip.FinalFare,
		CurrencyCode:       result.CurrencyCode,
		PaymentCollectedAt: now,
		EventID:            result.OngoingTrip.ID.String() + ":completed",
		PublishedAt:        time.Now().UTC(),
	}
	if correlationID != nil {
		event.CorrelationID = *correlationID
	}

	if err := s.producer.PublishRideCompleted(ctx, event); err != nil {
		return CollectPaymentResult{}, fmt.Errorf("publish ride completed request_id=%s: %w", event.RequestID, err)
	}

	return result, nil
}
