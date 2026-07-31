package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"go-ride-kafka-consumers/services/driver-request-handler/internal/auth"
	"go-ride-kafka-consumers/services/driver-request-handler/internal/config"
	"go-ride-kafka-consumers/services/driver-request-handler/internal/kafka"
	"go-ride-kafka-consumers/services/driver-request-handler/internal/offers"

	"github.com/google/uuid"
	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
)

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type ongoingTripPayload struct {
	TripRecordID       string     `json:"trip_record_id"`
	RequestID          string     `json:"request_id"`
	TripID             string     `json:"trip_id"`
	DriverID           string     `json:"driver_id"`
	Status             string     `json:"status"`
	PickupLat          float64    `json:"pickup_lat"`
	PickupLng          float64    `json:"pickup_lng"`
	DropoffLat         float64    `json:"dropoff_lat"`
	DropoffLng         float64    `json:"dropoff_lng"`
	AssignedAt         time.Time  `json:"assigned_at"`
	StartedAt          *time.Time `json:"started_at,omitempty"`
	EndedAt            *time.Time `json:"ended_at,omitempty"`
	CompletedAt        *time.Time `json:"completed_at,omitempty"`
	FinalFare          *float64   `json:"final_fare,omitempty"`
	PaymentStatus      *string    `json:"payment_status,omitempty"`
	PaymentCollectedAt *time.Time `json:"payment_collected_at,omitempty"`
}

type tripRequestPayload struct {
	RequestID  string       `json:"request_id"`
	TripID     string       `json:"trip_id"`
	Status     string       `json:"status"`
	PickupLat  float64      `json:"pickup_lat"`
	PickupLng  float64      `json:"pickup_lng"`
	DropoffLat float64      `json:"dropoff_lat"`
	DropoffLng float64      `json:"dropoff_lng"`
	Fare       *farePayload `json:"fare,omitempty"`
}

type farePayload struct {
	FareID       string  `json:"fare_id"`
	CurrencyCode string  `json:"currency_code"`
	TotalFare    float64 `json:"total_fare"`
}

type acceptOfferResponse struct {
	TripRequest tripRequestPayload `json:"trip_request"`
	OngoingTrip ongoingTripPayload `json:"ongoing_trip"`
}

type endTripResponse struct {
	OngoingTrip  ongoingTripPayload `json:"ongoing_trip"`
	CurrencyCode string             `json:"currency_code,omitempty"`
}

type collectPaymentResponse struct {
	OngoingTrip  ongoingTripPayload `json:"ongoing_trip"`
	CurrencyCode string             `json:"currency_code,omitempty"`
}

type cancelTripBody struct {
	Reason string `json:"reason,omitempty"`
}

type cancelTripResponse struct {
	Accepted            bool               `json:"accepted"`
	OngoingTrip         ongoingTripPayload `json:"ongoing_trip"`
	RequestID           string             `json:"request_id"`
	Stage               string             `json:"stage"`
	RedispatchTriggered bool               `json:"redispatch_triggered"`
	CancelledAt         time.Time          `json:"cancelled_at"`
}

type Server struct {
	cfg      config.Config
	verifier *auth.Verifier
	offers   *offers.Service
	producer kafka.Producer
	http     *http.Server
}

func NewServer(cfg config.Config, verifier *auth.Verifier, offerService *offers.Service, producer kafka.Producer) *Server {
	server := &Server{
		cfg:      cfg,
		verifier: verifier,
		offers:   offerService,
		producer: producer,
	}

	const apiPrefix = "/api/v1/driver-trips"

	mux := http.NewServeMux()
	mux.HandleFunc("GET "+apiPrefix+"/healthz", server.handleHealthz)
	mux.HandleFunc("POST "+apiPrefix+"/job-offers/{job_offer_id}/accept", server.handleAcceptOffer)
	mux.HandleFunc("POST "+apiPrefix+"/ongoing-trips/{ongoing_trip_id}/start", server.handleStartTrip)
	mux.HandleFunc("POST "+apiPrefix+"/ongoing-trips/{ongoing_trip_id}/end", server.handleEndTrip)
	mux.HandleFunc("POST "+apiPrefix+"/ongoing-trips/{ongoing_trip_id}/collect-payment", server.handleCollectPayment)
	mux.HandleFunc("POST "+apiPrefix+"/ongoing-trips/{ongoing_trip_id}/cancel", server.handleCancelTrip)

	server.http = &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return server
}

func (s *Server) Start(ctx context.Context) error {
	defer func() {
		if err := s.producer.Close(); err != nil {
			log.Printf("close kafka producer topic=%s: %v", s.cfg.AssignedTopic, err)
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("driver request handler listening addr=%s", s.cfg.HTTPAddr)
		if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("listen and serve: %w", err)
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := s.http.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown http server: %w", err)
		}
		return nil
	case err, ok := <-errCh:
		if !ok {
			return nil
		}
		return err
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleAcceptOffer(w http.ResponseWriter, r *http.Request) {
	token := bearerToken(r)
	if token == "" {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "missing bearer token")
		return
	}

	claims, err := s.verifier.Parse(token)
	if err != nil || claims.Role != auth.DriverRole {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid or non-driver token")
		return
	}

	driverID, err := uuid.Parse(claims.UserID)
	if err != nil {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid driver id in token")
		return
	}

	jobOfferID, err := uuid.Parse(r.PathValue("job_offer_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_job_offer_id", "job_offer_id must be a valid UUID")
		return
	}

	result, err := s.offers.AcceptOffer(r.Context(), jobOfferID, driverID)
	if err != nil {
		switch {
		case errors.Is(err, offers.ErrOfferNotFound):
			writeJSONError(w, http.StatusNotFound, "offer_not_found", "job offer was not found")
		case errors.Is(err, offers.ErrOfferForbidden):
			writeJSONError(w, http.StatusForbidden, "offer_forbidden", "job offer does not belong to this driver")
		case errors.Is(err, offers.ErrOfferNotWinnable):
			writeJSONError(w, http.StatusConflict, "offer_not_winnable", "job offer is no longer pending, expired, or the request was already assigned")
		default:
			log.Printf("accept offer job_offer_id=%s driver_id=%s: %v", jobOfferID, driverID, err)
			writeJSONError(w, http.StatusInternalServerError, "internal_error", "failed to accept job offer")
		}
		return
	}

	response := acceptOfferResponse{
		TripRequest: tripRequestPayload{
			RequestID:  result.TripRequest.ID.String(),
			TripID:     result.TripRequest.TripID.String(),
			Status:     result.TripRequest.Status,
			PickupLat:  result.TripRequest.PickupLat,
			PickupLng:  result.TripRequest.PickupLng,
			DropoffLat: result.TripRequest.DropoffLat,
			DropoffLng: result.TripRequest.DropoffLng,
		},
		OngoingTrip: ongoingTripPayloadFrom(result.OngoingTrip),
	}
	if result.Fare != nil {
		response.TripRequest.Fare = farePayloadFromFare(result.Fare)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
	log.Printf("driver accepted offer request_id=%s trip_id=%s driver_id=%s", response.TripRequest.RequestID, response.TripRequest.TripID, driverID)
}

func (s *Server) handleStartTrip(w http.ResponseWriter, r *http.Request) {
	token := bearerToken(r)
	if token == "" {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "missing bearer token")
		return
	}

	claims, err := s.verifier.Parse(token)
	if err != nil || claims.Role != auth.DriverRole {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid or non-driver token")
		return
	}

	driverID, err := uuid.Parse(claims.UserID)
	if err != nil {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid driver id in token")
		return
	}

	ongoingTripID, err := uuid.Parse(r.PathValue("ongoing_trip_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_ongoing_trip_id", "ongoing_trip_id must be a valid UUID")
		return
	}

	result, err := s.offers.StartTrip(r.Context(), ongoingTripID, driverID)
	if err != nil {
		switch {
		case errors.Is(err, offers.ErrTripNotFound):
			writeJSONError(w, http.StatusNotFound, "trip_not_found", "ongoing trip was not found")
		case errors.Is(err, offers.ErrTripForbidden):
			writeJSONError(w, http.StatusForbidden, "trip_forbidden", "ongoing trip does not belong to this driver")
		case errors.Is(err, offers.ErrTripNotStartable):
			writeJSONError(w, http.StatusConflict, "trip_not_startable", "trip is not in a startable state")
		default:
			log.Printf("start trip ongoing_trip_id=%s driver_id=%s: %v", ongoingTripID, driverID, err)
			writeJSONError(w, http.StatusInternalServerError, "internal_error", "failed to start trip")
		}
		return
	}

	response := ongoingTripPayloadFrom(result.OngoingTrip)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
	log.Printf("driver started trip ongoing_trip_id=%s trip_id=%s driver_id=%s", response.TripRecordID, response.TripID, driverID)
}

func (s *Server) handleEndTrip(w http.ResponseWriter, r *http.Request) {
	token := bearerToken(r)
	if token == "" {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "missing bearer token")
		return
	}

	claims, err := s.verifier.Parse(token)
	if err != nil || claims.Role != auth.DriverRole {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid or non-driver token")
		return
	}

	driverID, err := uuid.Parse(claims.UserID)
	if err != nil {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid driver id in token")
		return
	}

	ongoingTripID, err := uuid.Parse(r.PathValue("ongoing_trip_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_ongoing_trip_id", "ongoing_trip_id must be a valid UUID")
		return
	}

	result, err := s.offers.EndTrip(r.Context(), ongoingTripID, driverID)
	if err != nil {
		switch {
		case errors.Is(err, offers.ErrTripNotFound):
			writeJSONError(w, http.StatusNotFound, "trip_not_found", "ongoing trip was not found")
		case errors.Is(err, offers.ErrTripForbidden):
			writeJSONError(w, http.StatusForbidden, "trip_forbidden", "ongoing trip does not belong to this driver")
		case errors.Is(err, offers.ErrTripNotEndable):
			writeJSONError(w, http.StatusConflict, "trip_not_endable", "trip is not in an endable state")
		default:
			log.Printf("end trip ongoing_trip_id=%s driver_id=%s: %v", ongoingTripID, driverID, err)
			writeJSONError(w, http.StatusInternalServerError, "internal_error", "failed to end trip")
		}
		return
	}

	response := endTripResponse{
		OngoingTrip:  ongoingTripPayloadFrom(result.OngoingTrip),
		CurrencyCode: result.CurrencyCode,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
	log.Printf("driver ended trip ongoing_trip_id=%s trip_id=%s driver_id=%s", response.OngoingTrip.TripRecordID, response.OngoingTrip.TripID, driverID)
}

func (s *Server) handleCollectPayment(w http.ResponseWriter, r *http.Request) {
	token := bearerToken(r)
	if token == "" {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "missing bearer token")
		return
	}

	claims, err := s.verifier.Parse(token)
	if err != nil || claims.Role != auth.DriverRole {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid or non-driver token")
		return
	}

	driverID, err := uuid.Parse(claims.UserID)
	if err != nil {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid driver id in token")
		return
	}

	ongoingTripID, err := uuid.Parse(r.PathValue("ongoing_trip_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_ongoing_trip_id", "ongoing_trip_id must be a valid UUID")
		return
	}

	result, err := s.offers.CollectPayment(r.Context(), ongoingTripID, driverID)
	if err != nil {
		switch {
		case errors.Is(err, offers.ErrTripNotFound):
			writeJSONError(w, http.StatusNotFound, "trip_not_found", "ongoing trip was not found")
		case errors.Is(err, offers.ErrTripForbidden):
			writeJSONError(w, http.StatusForbidden, "trip_forbidden", "ongoing trip does not belong to this driver")
		case errors.Is(err, offers.ErrTripNotCollectable):
			writeJSONError(w, http.StatusConflict, "trip_not_collectable", "trip is not awaiting payment")
		default:
			log.Printf("collect payment ongoing_trip_id=%s driver_id=%s: %v", ongoingTripID, driverID, err)
			writeJSONError(w, http.StatusInternalServerError, "internal_error", "failed to collect payment")
		}
		return
	}

	response := collectPaymentResponse{
		OngoingTrip:  ongoingTripPayloadFrom(result.OngoingTrip),
		CurrencyCode: result.CurrencyCode,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
	log.Printf("driver collected payment ongoing_trip_id=%s trip_id=%s driver_id=%s", response.OngoingTrip.TripRecordID, response.OngoingTrip.TripID, driverID)
}

func (s *Server) handleCancelTrip(w http.ResponseWriter, r *http.Request) {
	token := bearerToken(r)
	if token == "" {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "missing bearer token")
		return
	}

	claims, err := s.verifier.Parse(token)
	if err != nil || claims.Role != auth.DriverRole {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid or non-driver token")
		return
	}

	driverID, err := uuid.Parse(claims.UserID)
	if err != nil {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid driver id in token")
		return
	}

	ongoingTripID, err := uuid.Parse(r.PathValue("ongoing_trip_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_ongoing_trip_id", "ongoing_trip_id must be a valid UUID")
		return
	}

	var body cancelTripBody
	if decodeErr := json.NewDecoder(r.Body).Decode(&body); decodeErr != nil && !errors.Is(decodeErr, io.EOF) {
		writeJSONError(w, http.StatusBadRequest, "invalid_request_body", "request body must be valid JSON")
		return
	}

	result, err := s.offers.CancelTrip(r.Context(), ongoingTripID, driverID, body.Reason)
	if err != nil {
		switch {
		case errors.Is(err, offers.ErrTripNotFound):
			writeJSONError(w, http.StatusNotFound, "trip_not_found", "ongoing trip was not found")
		case errors.Is(err, offers.ErrTripForbidden):
			writeJSONError(w, http.StatusForbidden, "trip_forbidden", "ongoing trip does not belong to this driver")
		case errors.Is(err, offers.ErrTripAlreadyCancelled):
			writeJSONError(w, http.StatusConflict, "trip_already_cancelled", "this trip has already been cancelled")
		case errors.Is(err, offers.ErrTripNotCancellable):
			writeJSONError(w, http.StatusConflict, "trip_not_cancellable", "this trip is no longer cancellable")
		default:
			log.Printf("cancel trip ongoing_trip_id=%s driver_id=%s: %v", ongoingTripID, driverID, err)
			writeJSONError(w, http.StatusInternalServerError, "internal_error", "failed to cancel trip")
		}
		return
	}

	response := cancelTripResponse{
		Accepted:            true,
		OngoingTrip:         ongoingTripPayloadFrom(result.OngoingTrip),
		RequestID:           result.TripRequest.ID.String(),
		Stage:               result.Stage,
		RedispatchTriggered: result.Redispatch,
		CancelledAt:         *result.OngoingTrip.CancelledAt,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
	log.Printf("driver cancelled trip ongoing_trip_id=%s trip_id=%s driver_id=%s stage=%s redispatch=%t", response.OngoingTrip.TripRecordID, response.OngoingTrip.TripID, driverID, response.Stage, response.RedispatchTriggered)
}

func ongoingTripPayloadFrom(trip schemamodels.OngoingTrip) ongoingTripPayload {
	return ongoingTripPayload{
		TripRecordID:       trip.ID.String(),
		RequestID:          trip.RequestID.String(),
		TripID:             trip.TripID.String(),
		DriverID:           trip.DriverID.String(),
		Status:             trip.Status,
		PickupLat:          trip.PickupLat,
		PickupLng:          trip.PickupLng,
		DropoffLat:         trip.DropoffLat,
		DropoffLng:         trip.DropoffLng,
		AssignedAt:         trip.AssignedAt,
		StartedAt:          trip.StartedAt,
		EndedAt:            trip.EndedAt,
		CompletedAt:        trip.CompletedAt,
		FinalFare:          trip.FinalFare,
		PaymentStatus:      trip.PaymentStatus,
		PaymentCollectedAt: trip.PaymentCollectedAt,
	}
}

func farePayloadFromFare(fare *schemamodels.TripFare) *farePayload {
	return &farePayload{
		FareID:       fare.ID.String(),
		CurrencyCode: fare.CurrencyCode,
		TotalFare:    fare.TotalFare,
	}
}

func bearerToken(r *http.Request) string {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(header, "Bearer ") {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
}

func writeJSONError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: code, Message: message})
}
