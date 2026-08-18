package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
	"github.com/shawon-kanji/go-ride-utils/events"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	errCancelRequestNotFound  = errors.New("trip_request_not_found")
	errCancelForbidden        = errors.New("trip_forbidden")
	errCancelAlreadyCancelled = errors.New("trip_already_cancelled")
	errCancelNotCancellable   = errors.New("trip_not_cancellable")
)

func isValidCancellationReason(reason string) bool {
	for _, valid := range schemamodels.ValidCancellationReasons() {
		if reason == valid {
			return true
		}
	}
	return false
}

// cancelCabRequestRequest's Reason is optional — unlike the driver's cancel
// screen (D10), the design has no reason-picker for a rider cancelling
// during search, so there's nothing forcing the client to send one. If
// present, it's still validated against the same fixed enum the driver path
// uses. Note is a separate, always-optional freeform field.
type cancelCabRequestRequest struct {
	Reason string `json:"reason,omitempty"`
	Note   string `json:"note,omitempty"`
}

type cancelCabRequestResponse struct {
	Accepted            bool      `json:"accepted"`
	RequestID           string    `json:"request_id"`
	TripID              string    `json:"trip_id"`
	Stage               string    `json:"stage"`
	RequestStatus       string    `json:"request_status"`
	OngoingTripStatus   string    `json:"ongoing_trip_status,omitempty"`
	CancelledAt         time.Time `json:"cancelled_at"`
	WithdrawnOfferCount int       `json:"withdrawn_offer_count,omitempty"`
	EventID             string    `json:"event_id"`
	PublishedAt         time.Time `json:"published_at"`
}

// cancelResult carries what cancelTripRequest changed, so the HTTP handler
// and the published event can be built from it without re-querying.
type cancelResult struct {
	request            schemamodels.TripRequest
	ongoingTrip        *schemamodels.OngoingTrip
	stage              string
	priorStatus        string
	withdrawnDriverIDs []string
}

func (s *Server) handleCancelTripRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	riderID, ok := s.authenticateRider(w, r)
	if !ok {
		return
	}

	requestID, err := uuid.Parse(r.PathValue("request_id"))
	if err != nil {
		http.Error(w, "request_id must be a valid UUID", http.StatusBadRequest)
		return
	}

	var req cancelCabRequestRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if req.Reason != "" && !isValidCancellationReason(req.Reason) {
		writeJSONError(w, http.StatusBadRequest, "invalid_cancellation_reason", "reason, if given, must be one of: "+strings.Join(schemamodels.ValidCancellationReasons(), ", "))
		return
	}

	now := time.Now().UTC()
	result, err := s.cancelTripRequest(r.Context(), requestID, riderID, req.Reason, req.Note, now)
	if err != nil {
		switch {
		case errors.Is(err, errCancelRequestNotFound):
			writeJSONError(w, http.StatusNotFound, "trip_request_not_found", "request_id was not found")
		case errors.Is(err, errCancelForbidden):
			writeJSONError(w, http.StatusForbidden, "trip_forbidden", "rider_id does not own this request")
		case errors.Is(err, errCancelAlreadyCancelled):
			writeJSONError(w, http.StatusConflict, "trip_already_cancelled", "this trip has already been cancelled")
		case errors.Is(err, errCancelNotCancellable):
			writeJSONError(w, http.StatusConflict, "trip_not_cancellable", "this trip is no longer cancellable")
		default:
			log.Printf("cancel trip request request_id=%s: %v", requestID, err)
			http.Error(w, "failed to cancel trip request", http.StatusInternalServerError)
		}
		return
	}

	event := s.newRideCancelledEvent(result, now)
	if err := s.producer.PublishRideCancelled(r.Context(), event); err != nil {
		log.Printf("publish ride cancelled request_id=%s: %v", requestID, err)
		http.Error(w, "failed to publish cancellation", http.StatusBadGateway)
		return
	}

	response := cancelCabRequestResponse{
		Accepted:            true,
		RequestID:           result.request.ID.String(),
		TripID:              result.request.TripID.String(),
		Stage:               result.stage,
		RequestStatus:       result.request.Status,
		CancelledAt:         now,
		WithdrawnOfferCount: len(result.withdrawnDriverIDs),
		EventID:             event.EventID,
		PublishedAt:         event.PublishedAt,
	}
	if result.ongoingTrip != nil {
		response.OngoingTripStatus = result.ongoingTrip.Status
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
	log.Printf("cancelled trip request request_id=%s stage=%s withdrawn_offers=%d", response.RequestID, response.Stage, response.WithdrawnOfferCount)
}

// cancelTripRequest implements rider-initiated cancellation across all three
// cancellable stages in one transaction: search (trip_requests only, plus
// withdrawing any pending driver_job_offers), and assigned/in_progress
// (ongoing_trips, mirrored onto trip_requests). Row-locks trip_requests
// first, matching AcceptOffer's own lock ordering (offer-then-request) --
// this handler never holds an offer lock while waiting on a request lock a
// second time, so the two can't deadlock against each other.
func (s *Server) cancelTripRequest(ctx context.Context, requestID, riderID uuid.UUID, reason, note string, now time.Time) (cancelResult, error) {
	var result cancelResult

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var request schemamodels.TripRequest
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("request_id = ?", requestID).
			Take(&request).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return errCancelRequestNotFound
		}
		if err != nil {
			return fmt.Errorf("lock trip request request_id=%s: %w", requestID, err)
		}

		if request.RiderID != riderID {
			return errCancelForbidden
		}
		if request.Status == schemamodels.TripRequestStatusCancelled {
			return errCancelAlreadyCancelled
		}
		if request.Status == schemamodels.TripRequestStatusTimedOut {
			return errCancelNotCancellable
		}

		reasonPtr := stringPtr(reason)
		notePtr := stringPtr(note)
		cancelledBy := schemamodels.CancelledByRider

		if request.Status != schemamodels.TripRequestStatusAssigned {
			return s.cancelDuringSearch(tx, &result, request, reasonPtr, notePtr, cancelledBy, now)
		}
		return s.cancelAfterAssignment(tx, &result, request, reasonPtr, notePtr, cancelledBy, now)
	})
	if err != nil {
		return cancelResult{}, err
	}

	return result, nil
}

// cancelDuringSearch handles a cancel while trip_requests is still the
// authoritative table (search_started/searching/offered/driver_accepted):
// withdraws any pending job offers and flips trip_requests to cancelled.
func (s *Server) cancelDuringSearch(tx *gorm.DB, result *cancelResult, request schemamodels.TripRequest, reasonPtr, notePtr *string, cancelledBy string, now time.Time) error {
	var offers []schemamodels.DriverJobOffer
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("request_id = ? AND status = ?", request.ID, schemamodels.DriverJobOfferStatusPending).
		Order("job_offer_id ASC").
		Find(&offers).Error; err != nil {
		return fmt.Errorf("lock pending offers request_id=%s: %w", request.ID, err)
	}

	var offerIDs []uuid.UUID
	for _, offer := range offers {
		offerIDs = append(offerIDs, offer.ID)
		result.withdrawnDriverIDs = append(result.withdrawnDriverIDs, offer.DriverID.String())
	}
	if len(offerIDs) > 0 {
		if err := tx.Model(&schemamodels.DriverJobOffer{}).
			Where("job_offer_id IN ?", offerIDs).
			Updates(map[string]any{"status": schemamodels.DriverJobOfferStatusWithdrawn, "withdrawn_at": now, "updated_at": now}).Error; err != nil {
			return fmt.Errorf("withdraw pending offers request_id=%s: %w", request.ID, err)
		}
	}

	fromStatus := request.Status
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

	payload, err := json.Marshal(map[string]any{
		"withdrawn_offer_ids":  offerIDs,
		"withdrawn_driver_ids": result.withdrawnDriverIDs,
	})
	if err != nil {
		return fmt.Errorf("marshal trip history payload: %w", err)
	}
	if err := createCancelHistory(tx, request, nil, "trip_request_cancelled", fromStatus, payload, now); err != nil {
		return err
	}

	result.request = request
	result.stage = "search"
	result.priorStatus = fromStatus
	return nil
}

// cancelAfterAssignment handles a cancel once ongoing_trips is the
// authoritative table (assigned/driver_arriving/in_progress): flips
// ongoing_trips to cancelled and mirrors trip_requests to match.
func (s *Server) cancelAfterAssignment(tx *gorm.DB, result *cancelResult, request schemamodels.TripRequest, reasonPtr, notePtr *string, cancelledBy string, now time.Time) error {
	var ongoingTrip schemamodels.OngoingTrip
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("request_id = ?", request.ID).
		Take(&ongoingTrip).Error; err != nil {
		return fmt.Errorf("lock ongoing trip request_id=%s: %w", request.ID, err)
	}

	var stage string
	switch ongoingTrip.Status {
	case schemamodels.OngoingTripStatusAssigned, schemamodels.OngoingTripStatusDriverArriving:
		stage = "assigned"
	case schemamodels.OngoingTripStatusInProgress:
		stage = "in_progress"
	case schemamodels.OngoingTripStatusCancelled:
		return errCancelAlreadyCancelled
	default:
		return errCancelNotCancellable
	}

	fromStatus := ongoingTrip.Status
	if err := tx.Model(&schemamodels.OngoingTrip{}).
		Where("id = ?", ongoingTrip.ID).
		Updates(map[string]any{
			"status":              schemamodels.OngoingTripStatusCancelled,
			"cancelled_at":        now,
			"cancellation_reason": reasonPtr,
			"cancellation_note":   notePtr,
			"cancelled_by":        cancelledBy,
			"updated_at":          now,
		}).Error; err != nil {
		return fmt.Errorf("cancel ongoing trip ongoing_trip_id=%s: %w", ongoingTrip.ID, err)
	}
	ongoingTrip.Status = schemamodels.OngoingTripStatusCancelled
	ongoingTrip.CancelledAt = &now
	ongoingTrip.CancellationReason = reasonPtr
	ongoingTrip.CancellationNote = notePtr
	ongoingTrip.CancelledBy = &cancelledBy

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

	payload, err := json.Marshal(map[string]any{"ongoing_trip_id": ongoingTrip.ID})
	if err != nil {
		return fmt.Errorf("marshal trip history payload: %w", err)
	}
	if err := createCancelHistory(tx, request, &ongoingTrip.DriverID, "trip_cancelled", fromStatus, payload, now); err != nil {
		return err
	}

	result.request = request
	result.ongoingTrip = &ongoingTrip
	result.stage = stage
	result.priorStatus = fromStatus
	return nil
}

func createCancelHistory(tx *gorm.DB, request schemamodels.TripRequest, driverID *uuid.UUID, eventType, fromStatus string, payload json.RawMessage, now time.Time) error {
	toStatus := schemamodels.TripRequestStatusCancelled
	history := schemamodels.TripHistory{
		ID:           uuid.New(),
		RequestID:    request.ID,
		TripID:       request.TripID,
		RiderID:      &request.RiderID,
		DriverID:     driverID,
		EventType:    eventType,
		FromStatus:   &fromStatus,
		ToStatus:     &toStatus,
		EventPayload: payload,
		CreatedAt:    now,
	}
	if request.CorrelationID != nil {
		history.CorrelationID = request.CorrelationID
	}
	return tx.Create(&history).Error
}

func (s *Server) newRideCancelledEvent(result cancelResult, now time.Time) events.RideCancelledV1 {
	event := events.RideCancelledV1{
		RequestID:               result.request.ID.String(),
		TripID:                  result.request.TripID.String(),
		RiderID:                 result.request.RiderID.String(),
		Stage:                   result.stage,
		PriorStatus:             result.priorStatus,
		WithdrawnOfferDriverIDs: result.withdrawnDriverIDs,
		CancellationReason:      valueOrEmpty(result.request.CancellationReason),
		CancellationNote:        valueOrEmpty(result.request.CancellationNote),
		CancelledBy:             valueOrEmpty(result.request.CancelledBy),
		CancelledAt:             now,
		CorrelationID:           valueOrEmpty(result.request.CorrelationID),
		EventID:                 result.request.ID.String() + ":cancelled",
		PublishedAt:             now,
	}
	if result.ongoingTrip != nil {
		event.OngoingTripID = result.ongoingTrip.ID.String()
		event.DriverID = result.ongoingTrip.DriverID.String()
	}
	return event
}
