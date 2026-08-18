package offers

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/shawon-kanji/go-ride-utils/events"
	"go-ride-kafka-consumers/services/driver-request-handler/internal/kafka"

	"github.com/google/uuid"
	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrOfferNotFound    = errors.New("offer_not_found")
	ErrOfferForbidden   = errors.New("offer_forbidden")
	ErrOfferNotWinnable = errors.New("offer_not_winnable")

	ErrTripNotFound         = errors.New("trip_not_found")
	ErrTripForbidden        = errors.New("trip_forbidden")
	ErrTripNotStartable     = errors.New("trip_not_startable")
	ErrInvalidStartPin      = errors.New("invalid_start_pin")
	ErrTripNotEndable       = errors.New("trip_not_endable")
	ErrTripNotCollectable   = errors.New("trip_not_collectable")
	ErrTripAlreadyCancelled = errors.New("trip_already_cancelled")
	ErrTripNotCancellable   = errors.New("trip_not_cancellable")
)

type AcceptResult struct {
	TripRequest     schemamodels.TripRequest
	OngoingTrip     schemamodels.OngoingTrip
	Fare            *schemamodels.TripFare
	Vehicle         *schemamodels.Vehicle
	WithdrawnOffers []schemamodels.DriverJobOffer
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

type CancelTripResult struct {
	OngoingTrip schemamodels.OngoingTrip
	TripRequest schemamodels.TripRequest
	Stage       string
	PriorStatus string
	Redispatch  bool
}

type CurrentTripResult struct {
	OngoingTrip *schemamodels.OngoingTrip
	TripRequest *schemamodels.TripRequest
	Fare        *schemamodels.TripFare
}

// TripHistoryRow is the Scan target for ListTrips' raw query — explicit
// column tags rather than relying on snake_case inference, matching
// trip-dispatch-worker's own driverCandidate precedent for Raw().Scan().
// Unlike CurrentTrip, a driver's history is purely ongoing_trips rows
// (a driver never "has" a trip that wasn't assigned to them), so this
// needs no trip_requests fallback the way cab-request-handler's rider-side
// equivalent does.
type TripHistoryRow struct {
	OngoingTripID uuid.UUID  `gorm:"column:ongoing_trip_id"`
	RequestID     uuid.UUID  `gorm:"column:request_id"`
	TripID        uuid.UUID  `gorm:"column:trip_id"`
	RiderID       uuid.UUID  `gorm:"column:rider_id"`
	Status        string     `gorm:"column:status"`
	PickupLat     float64    `gorm:"column:pickup_lat"`
	PickupLng     float64    `gorm:"column:pickup_lng"`
	DropoffLat    float64    `gorm:"column:dropoff_lat"`
	DropoffLng    float64    `gorm:"column:dropoff_lng"`
	AssignedAt    time.Time  `gorm:"column:assigned_at"`
	CompletedAt   *time.Time `gorm:"column:completed_at"`
	CancelledAt   *time.Time `gorm:"column:cancelled_at"`
	FinalFare     *float64   `gorm:"column:final_fare"`
	CurrencyCode  *string    `gorm:"column:currency_code"`
}

type Service struct {
	db       *gorm.DB
	producer kafka.Producer
}

func NewService(db *gorm.DB, producer kafka.Producer) *Service {
	return &Service{db: db, producer: producer}
}

// generateStartPin returns a zero-padded 4-digit PIN, e.g. "0042".
func generateStartPin() (string, error) {
	n, err := cryptorand.Int(cryptorand.Reader, big.NewInt(10000))
	if err != nil {
		return "", fmt.Errorf("generate start pin: %w", err)
	}
	return fmt.Sprintf("%04d", n.Int64()), nil
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

		// Losing offers are marked withdrawn (not expired): "expired" is
		// reserved for an offer whose TTL simply ran out with no winner at
		// all, which the driver app already renders via its own countdown.
		// "Withdrawn" is the distinct, event-carrying case of "someone else
		// took it" — locked and captured here (rather than a blind bulk
		// UPDATE) so the caller has the exact (job_offer_id, driver_id)
		// pairs to publish on JobOfferWithdrawnV1 after commit.
		var losingOffers []schemamodels.DriverJobOffer
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("request_id = ? AND job_offer_id <> ? AND status = ?", offer.RequestID, offer.ID, schemamodels.DriverJobOfferStatusPending).
			Order("job_offer_id ASC").
			Find(&losingOffers).Error; err != nil {
			return fmt.Errorf("lock losing offers request_id=%s: %w", offer.RequestID, err)
		}
		if len(losingOffers) > 0 {
			losingIDs := make([]uuid.UUID, len(losingOffers))
			for i, lo := range losingOffers {
				losingIDs[i] = lo.ID
			}
			if err := tx.Model(&schemamodels.DriverJobOffer{}).
				Where("job_offer_id IN ?", losingIDs).
				Updates(map[string]any{"status": schemamodels.DriverJobOfferStatusWithdrawn, "withdrawn_at": now, "updated_at": now}).Error; err != nil {
				return fmt.Errorf("withdraw losing offers request_id=%s: %w", offer.RequestID, err)
			}
		}
		result.WithdrawnOffers = losingOffers

		if err := tx.Model(&schemamodels.TripRequest{}).
			Where("request_id = ?", request.ID).
			Updates(map[string]any{"status": schemamodels.TripRequestStatusAssigned, "assigned_at": now, "updated_at": now}).Error; err != nil {
			return fmt.Errorf("mark trip request assigned: %w", err)
		}
		request.Status = schemamodels.TripRequestStatusAssigned
		request.AssignedAt = &now

		var vehicle *schemamodels.Vehicle
		var vehicleID *uuid.UUID
		var activeVehicle schemamodels.Vehicle
		if err := tx.Where("driver_id = ? AND is_active = true", driverID).Take(&activeVehicle).Error; err == nil {
			vehicle = &activeVehicle
			vehicleID = &activeVehicle.ID
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("load active vehicle driver_id=%s: %w", driverID, err)
		}

		// startPin is what the rider reads out to the driver at pickup and
		// the driver types into StartTrip to prove they're at the right car
		// — regenerated on every assignment (including redispatch reuse
		// below) so a fired driver's PIN never carries over to whoever picks
		// the request up next.
		startPin, err := generateStartPin()
		if err != nil {
			return err
		}

		// ongoing_trips.request_id is uniquely constrained for the request's
		// entire lifetime, so a request that's been redispatched after a prior
		// driver cancelled (its ongoing_trips row already exists, in status
		// cancelled) can't get a second row via a plain INSERT. Reuse that row
		// in place for the new assignment instead — the full history of prior
		// assignments already lives in trip_history, not here.
		var ongoingTrip schemamodels.OngoingTrip
		var existingTrip schemamodels.OngoingTrip
		err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("request_id = ?", request.ID).
			Take(&existingTrip).Error
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			ongoingTrip = schemamodels.OngoingTrip{
				ID:         uuid.New(),
				RequestID:  request.ID,
				TripID:     request.TripID,
				RiderID:    request.RiderID,
				DriverID:   driverID,
				VehicleID:  vehicleID,
				Status:     schemamodels.OngoingTripStatusAssigned,
				PickupLat:  request.PickupLat,
				PickupLng:  request.PickupLng,
				DropoffLat: request.DropoffLat,
				DropoffLng: request.DropoffLng,
				AssignedAt: now,
				StartPin:   &startPin,
				CreatedAt:  now,
				UpdatedAt:  now,
			}
			if err := tx.Create(&ongoingTrip).Error; err != nil {
				return fmt.Errorf("create ongoing trip: %w", err)
			}
		case err != nil:
			return fmt.Errorf("lock existing ongoing trip request_id=%s: %w", request.ID, err)
		default:
			if err := tx.Model(&schemamodels.OngoingTrip{}).
				Where("id = ?", existingTrip.ID).
				Updates(map[string]any{
					"driver_id":            driverID,
					"vehicle_id":           vehicleID,
					"status":               schemamodels.OngoingTripStatusAssigned,
					"assigned_at":          now,
					"start_pin":            startPin,
					"started_at":           nil,
					"ended_at":             nil,
					"completed_at":         nil,
					"cancelled_at":         nil,
					"cancellation_reason":  nil,
					"cancelled_by":         nil,
					"final_fare":           nil,
					"payment_status":       nil,
					"payment_collected_at": nil,
					"updated_at":           now,
				}).Error; err != nil {
				return fmt.Errorf("reassign ongoing trip request_id=%s: %w", request.ID, err)
			}
			ongoingTrip = existingTrip
			ongoingTrip.DriverID = driverID
			ongoingTrip.VehicleID = vehicleID
			ongoingTrip.Status = schemamodels.OngoingTripStatusAssigned
			ongoingTrip.AssignedAt = now
			ongoingTrip.StartPin = &startPin
			ongoingTrip.StartedAt = nil
			ongoingTrip.EndedAt = nil
			ongoingTrip.CompletedAt = nil
			ongoingTrip.CancelledAt = nil
			ongoingTrip.CancellationReason = nil
			ongoingTrip.CancelledBy = nil
			ongoingTrip.FinalFare = nil
			ongoingTrip.PaymentStatus = nil
			ongoingTrip.PaymentCollectedAt = nil
			ongoingTrip.UpdatedAt = now
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

		result.TripRequest = request
		result.OngoingTrip = ongoingTrip
		result.Fare = fare
		result.Vehicle = vehicle
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
	if result.OngoingTrip.StartPin != nil {
		event.StartPin = *result.OngoingTrip.StartPin
	}
	if result.Fare != nil {
		event.RouteDistanceKM = result.Fare.RouteDistanceKM
		event.RouteDurationMinutes = result.Fare.RouteDurationMinutes
	}
	if result.Vehicle != nil {
		event.VehicleColor = result.Vehicle.Color
		event.VehiclePlate = result.Vehicle.PlateNumber
		event.VehicleModel = result.Vehicle.ModelName
	}
	if result.TripRequest.CorrelationID != nil {
		event.CorrelationID = *result.TripRequest.CorrelationID
	}

	if err := s.producer.PublishRideAssigned(ctx, event); err != nil {
		return AcceptResult{}, fmt.Errorf("publish ride assigned request_id=%s: %w", event.RequestID, err)
	}

	if len(result.WithdrawnOffers) > 0 {
		withdrawnEvent := events.JobOfferWithdrawnV1{
			RequestID:     result.TripRequest.ID.String(),
			TripID:        result.TripRequest.TripID.String(),
			Offers:        make([]events.JobOfferWithdrawnEntry, len(result.WithdrawnOffers)),
			CorrelationID: event.CorrelationID,
			EventID:       result.OngoingTrip.ID.String() + ":withdrawn",
			PublishedAt:   time.Now().UTC(),
		}
		for i, lo := range result.WithdrawnOffers {
			withdrawnEvent.Offers[i] = events.JobOfferWithdrawnEntry{
				JobOfferID: lo.ID.String(),
				DriverID:   lo.DriverID.String(),
			}
		}
		if err := s.producer.PublishJobOfferWithdrawn(ctx, withdrawnEvent); err != nil {
			return AcceptResult{}, fmt.Errorf("publish job offer withdrawn request_id=%s: %w", withdrawnEvent.RequestID, err)
		}
	}

	return result, nil
}

// StartTrip marks a trip started once the driver has reached the rider and
// they're onboard: a single collapsed assigned -> in_progress transition, no
// separate "driver_arriving"/"arrived" step and no geofence validation
// against pickup coordinates — same trust-the-driver's-tap model as accept,
// except for the PIN check, which is the one thing that does confirm the
// driver is actually with the right rider (the rider reads it off their
// app and the driver types it in).
func (s *Service) StartTrip(ctx context.Context, ongoingTripID, driverID uuid.UUID, startPin string) (StartTripResult, error) {
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
		if trip.StartPin == nil || *trip.StartPin != startPin {
			return ErrInvalidStartPin
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

	var vehicleColor, vehiclePlate, vehicleModel string
	if result.OngoingTrip.VehicleID != nil {
		var vehicle schemamodels.Vehicle
		if err := s.db.WithContext(ctx).Where("id = ?", *result.OngoingTrip.VehicleID).Take(&vehicle).Error; err == nil {
			vehicleColor = vehicle.Color
			vehiclePlate = vehicle.PlateNumber
			vehicleModel = vehicle.ModelName
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return StartTripResult{}, fmt.Errorf("load vehicle vehicle_id=%s: %w", result.OngoingTrip.VehicleID.String(), err)
		}
	}

	event := events.RideStartedV1{
		RequestID:     result.OngoingTrip.RequestID.String(),
		TripID:        result.OngoingTrip.TripID.String(),
		OngoingTripID: result.OngoingTrip.ID.String(),
		RiderID:       result.OngoingTrip.RiderID.String(),
		DriverID:      driverID.String(),
		StartedAt:     now,
		VehicleColor:  vehicleColor,
		VehiclePlate:  vehiclePlate,
		VehicleModel:  vehicleModel,
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

// CancelTrip cancels a trip the driver has already accepted, from acceptance
// (assigned/driver_arriving) through an in-progress trip. Unlike the rider
// cancellation flow, there is no search-stage branch here: a driver only ever
// attaches to a trip via AcceptOffer, so ongoing_trips is always the
// authoritative table.
//
// Locking order deliberately matches rider-cancellation's cancelAfterAssignment
// (trip_requests locked first, then ongoing_trips) so this can never deadlock
// against a concurrent rider cancel on the same trip. The URL/caller only has
// ongoing_trip_id though, so request_id is discovered with a plain (unlocked)
// read first, purely to know what to lock in order; the real validation
// happens on the locked re-read of ongoing_trips below.
//
// When the trip is in_progress (mid-trip cancellation), this also records the
// straight-line distance between the driver's last-known location and the
// dropoff point on the trip_history row — a safety audit fact, not acted on
// here (no SOS/alerting; that's future work).
func (s *Service) CancelTrip(ctx context.Context, ongoingTripID, driverID uuid.UUID, reason, note string) (CancelTripResult, error) {
	now := time.Now().UTC()
	var result CancelTripResult

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var pre schemamodels.OngoingTrip
		if err := tx.Select("request_id").Where("id = ?", ongoingTripID).Take(&pre).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrTripNotFound
			}
			return fmt.Errorf("lookup ongoing trip id=%s: %w", ongoingTripID, err)
		}

		var request schemamodels.TripRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("request_id = ?", pre.RequestID).
			Take(&request).Error; err != nil {
			return fmt.Errorf("lock trip request request_id=%s: %w", pre.RequestID, err)
		}

		var trip schemamodels.OngoingTrip
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", ongoingTripID).
			Take(&trip).Error; err != nil {
			return fmt.Errorf("lock ongoing trip id=%s: %w", ongoingTripID, err)
		}

		if trip.DriverID != driverID {
			return ErrTripForbidden
		}

		var stage string
		switch trip.Status {
		case schemamodels.OngoingTripStatusAssigned, schemamodels.OngoingTripStatusDriverArriving:
			stage = "assigned"
		case schemamodels.OngoingTripStatusInProgress:
			stage = "in_progress"
		case schemamodels.OngoingTripStatusCancelled:
			return ErrTripAlreadyCancelled
		default:
			return ErrTripNotCancellable
		}

		fromStatus := trip.Status
		cancelledBy := schemamodels.CancelledByDriver
		var reasonPtr, notePtr *string
		if reason != "" {
			reasonPtr = &reason
		}
		if note != "" {
			notePtr = &note
		}

		if err := tx.Model(&schemamodels.OngoingTrip{}).
			Where("id = ?", trip.ID).
			Updates(map[string]any{
				"status":              schemamodels.OngoingTripStatusCancelled,
				"cancelled_at":        now,
				"cancellation_reason": reasonPtr,
				"cancellation_note":   notePtr,
				"cancelled_by":        cancelledBy,
				"updated_at":          now,
			}).Error; err != nil {
			return fmt.Errorf("cancel ongoing trip id=%s: %w", trip.ID, err)
		}
		trip.Status = schemamodels.OngoingTripStatusCancelled
		trip.CancelledAt = &now
		trip.CancellationReason = reasonPtr
		trip.CancellationNote = notePtr
		trip.CancelledBy = &cancelledBy

		if err := tx.Model(&schemamodels.TripRequest{}).
			Where("request_id = ?", request.ID).
			Updates(map[string]any{
				"status":              schemamodels.TripRequestStatusCancelled,
				"cancelled_at":        now,
				"cancellation_reason": reasonPtr,
				"cancellation_note":   notePtr,
				"cancelled_by":        cancelledBy,
				"updated_at":          now,
			}).Error; err != nil {
			return fmt.Errorf("cancel trip request request_id=%s: %w", request.ID, err)
		}
		request.Status = schemamodels.TripRequestStatusCancelled
		request.CancelledAt = &now
		request.CancellationReason = reasonPtr
		request.CancellationNote = notePtr
		request.CancelledBy = &cancelledBy

		payload := map[string]any{"ongoing_trip_id": trip.ID}
		if stage == "in_progress" {
			var location schemamodels.DriverLocation
			if err := tx.Where("driver_id = ?", driverID).Take(&location).Error; err == nil {
				distanceKM := haversineKM(location.Latitude, location.Longitude, trip.DropoffLat, trip.DropoffLng)
				payload["dropoff_distance_km"] = distanceKM
				payload["driver_lat"] = location.Latitude
				payload["driver_lng"] = location.Longitude
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("load driver location driver_id=%s: %w", driverID, err)
			}
		}

		eventPayload, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal trip history payload: %w", err)
		}
		toStatus := schemamodels.TripRequestStatusCancelled
		history := schemamodels.TripHistory{
			ID:            uuid.New(),
			RequestID:     request.ID,
			TripID:        request.TripID,
			RiderID:       &request.RiderID,
			DriverID:      &driverID,
			EventType:     "driver_cancelled",
			FromStatus:    &fromStatus,
			ToStatus:      &toStatus,
			EventPayload:  eventPayload,
			CorrelationID: request.CorrelationID,
			CreatedAt:     now,
		}
		if err := tx.Create(&history).Error; err != nil {
			return fmt.Errorf("create trip history: %w", err)
		}

		result = CancelTripResult{
			OngoingTrip: trip,
			TripRequest: request,
			Stage:       stage,
			PriorStatus: fromStatus,
			Redispatch:  stage == "assigned",
		}
		return nil
	})
	if err != nil {
		return CancelTripResult{}, err
	}

	event := events.RideCancelledV1{
		RequestID:     result.TripRequest.ID.String(),
		TripID:        result.TripRequest.TripID.String(),
		RiderID:       result.TripRequest.RiderID.String(),
		Stage:         result.Stage,
		PriorStatus:   result.PriorStatus,
		DriverID:      driverID.String(),
		OngoingTripID: result.OngoingTrip.ID.String(),
		CancelledBy:   schemamodels.CancelledByDriver,
		CancelledAt:   now,
		EventID:       result.OngoingTrip.ID.String() + ":cancelled",
		PublishedAt:   time.Now().UTC(),
	}
	if result.TripRequest.CancellationReason != nil {
		event.CancellationReason = *result.TripRequest.CancellationReason
	}
	if result.TripRequest.CancellationNote != nil {
		event.CancellationNote = *result.TripRequest.CancellationNote
	}
	if result.TripRequest.CorrelationID != nil {
		event.CorrelationID = *result.TripRequest.CorrelationID
	}

	if err := s.producer.PublishRideCancelled(ctx, event); err != nil {
		return CancelTripResult{}, fmt.Errorf("publish ride cancelled request_id=%s: %w", event.RequestID, err)
	}

	return result, nil
}

// activeOngoingTripStatuses mirrors cab-request-handler's own list (the
// rider-side equivalent of this recovery endpoint) — every status where a
// driver still has a trip to act on.
var activeOngoingTripStatuses = []string{
	schemamodels.OngoingTripStatusAssigned,
	schemamodels.OngoingTripStatusDriverArriving,
	schemamodels.OngoingTripStatusInProgress,
	schemamodels.OngoingTripStatusAwaitingPayment,
}

var terminalOngoingTripStatuses = []string{
	schemamodels.OngoingTripStatusCompleted,
	schemamodels.OngoingTripStatusCancelled,
}

// ListTrips returns driverID's terminal trips (completed or cancelled)
// newest first, keyset-paginated on (assigned_at, id) rather than OFFSET so
// a page boundary never shifts under concurrent inserts.
func (s *Service) ListTrips(ctx context.Context, driverID uuid.UUID, cursorTime time.Time, cursorID uuid.UUID, hasCursor bool, limit int) ([]TripHistoryRow, error) {
	query := `
		SELECT
			ot.id AS ongoing_trip_id, ot.request_id, ot.trip_id, ot.rider_id,
			ot.pickup_lat, ot.pickup_lng, ot.dropoff_lat, ot.dropoff_lng,
			ot.status, ot.assigned_at, ot.completed_at, ot.cancelled_at, ot.final_fare,
			tf.currency_code
		FROM ongoing_trips ot
		LEFT JOIN trip_requests tr ON tr.request_id = ot.request_id
		LEFT JOIN trip_fares tf ON tf.fare_id = tr.fare_id
		WHERE ot.driver_id = ?
		  AND ot.status IN (?)
	`
	args := []any{driverID, terminalOngoingTripStatuses}
	if hasCursor {
		query += " AND (ot.assigned_at, ot.id) < (?, ?)"
		args = append(args, cursorTime, cursorID)
	}
	query += " ORDER BY ot.assigned_at DESC, ot.id DESC LIMIT ?"
	args = append(args, limit)

	var rows []TripHistoryRow
	if err := s.db.WithContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("query driver trip history driver_id=%s: %w", driverID, err)
	}
	return rows, nil
}

// EarningsRow is one completed trip's contribution to an earnings query —
// gross cash collected (final_fare), not commission-adjusted: cash-only
// settlement means final_fare is exactly what the driver physically holds
// after the trip, unlike JobOfferV1's pre-accept estimated_earning, which
// forecasts the driver's eventual net take before a commission is settled
// (a separate, out-of-band process for cash trips).
type EarningsRow struct {
	FinalFare    float64   `gorm:"column:final_fare"`
	CurrencyCode *string   `gorm:"column:currency_code"`
	CompletedAt  time.Time `gorm:"column:completed_at"`
}

// EarningsSince returns every completed trip (payment collected) for
// driverID with completed_at >= since, oldest first. Only "completed" status
// counts as earned — "awaiting_payment" means cash hasn't actually been
// collected yet, so it isn't money the driver has in hand.
func (s *Service) EarningsSince(ctx context.Context, driverID uuid.UUID, since time.Time) ([]EarningsRow, error) {
	query := `
		SELECT ot.final_fare, tf.currency_code, ot.completed_at
		FROM ongoing_trips ot
		LEFT JOIN trip_requests tr ON tr.request_id = ot.request_id
		LEFT JOIN trip_fares tf ON tf.fare_id = tr.fare_id
		WHERE ot.driver_id = ?
		  AND ot.status = ?
		  AND ot.completed_at >= ?
		ORDER BY ot.completed_at ASC
	`
	var rows []EarningsRow
	err := s.db.WithContext(ctx).Raw(query, driverID, schemamodels.OngoingTripStatusCompleted, since).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("query driver earnings driver_id=%s: %w", driverID, err)
	}
	return rows, nil
}

// OnlineSessionRow is one driver_online_sessions row possibly overlapping a
// query window — EndedAt nil means still open (ongoing as of "now").
type OnlineSessionRow struct {
	StartedAt time.Time  `gorm:"column:started_at"`
	EndedAt   *time.Time `gorm:"column:ended_at"`
}

// OnlineSessionsOverlapping returns every session for driverID that
// overlaps [since, now] at all — including one that started before since
// (still online from earlier) or is still open (EndedAt nil). Callers clip
// each row to the window themselves; a session spanning multiple days needs
// per-day clipping too; see the api package's bucketOnlineTime.
func (s *Service) OnlineSessionsOverlapping(ctx context.Context, driverID uuid.UUID, since, now time.Time) ([]OnlineSessionRow, error) {
	query := `
		SELECT started_at, ended_at
		FROM driver_online_sessions
		WHERE driver_id = ?
		  AND started_at <= ?
		  AND COALESCE(ended_at, ?) >= ?
		ORDER BY started_at ASC
	`
	var rows []OnlineSessionRow
	err := s.db.WithContext(ctx).Raw(query, driverID, now, now, since).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("query driver online sessions driver_id=%s: %w", driverID, err)
	}
	return rows, nil
}

// CurrentTrip lets a driver who force-quit or crashed mid-trip recover which
// trip they're on, its status, and the rider's fare — the driver-side
// mirror of cab-request-handler's GET /current-trip. Never errors on
// "nothing found"; callers check CurrentTripResult.OngoingTrip == nil.
func (s *Service) CurrentTrip(ctx context.Context, driverID uuid.UUID) (CurrentTripResult, error) {
	var trip schemamodels.OngoingTrip
	err := s.db.WithContext(ctx).
		Where("driver_id = ? AND status IN ?", driverID, activeOngoingTripStatuses).
		Order("assigned_at DESC").
		Take(&trip).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return CurrentTripResult{}, nil
	}
	if err != nil {
		return CurrentTripResult{}, fmt.Errorf("query current trip driver_id=%s: %w", driverID, err)
	}

	var request schemamodels.TripRequest
	if err := s.db.WithContext(ctx).Where("request_id = ?", trip.RequestID).Take(&request).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return CurrentTripResult{}, fmt.Errorf("load trip request request_id=%s: %w", trip.RequestID, err)
	} else if errors.Is(err, gorm.ErrRecordNotFound) {
		return CurrentTripResult{OngoingTrip: &trip}, nil
	}

	result := CurrentTripResult{OngoingTrip: &trip, TripRequest: &request}
	if request.FareID != nil {
		var fare schemamodels.TripFare
		if err := s.db.WithContext(ctx).Where("fare_id = ?", *request.FareID).Take(&fare).Error; err == nil {
			result.Fare = &fare
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return CurrentTripResult{}, fmt.Errorf("load trip fare fare_id=%s: %w", request.FareID.String(), err)
		}
	}

	return result, nil
}
