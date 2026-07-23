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
)

type AcceptResult struct {
	TripRequest schemamodels.TripRequest
	OngoingTrip schemamodels.OngoingTrip
	Fare        *schemamodels.TripFare
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
