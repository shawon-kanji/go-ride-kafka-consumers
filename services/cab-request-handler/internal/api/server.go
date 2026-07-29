package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	"go-ride-kafka-consumers/services/cab-request-handler/internal/config"
	"go-ride-kafka-consumers/services/cab-request-handler/internal/kafka"

	"github.com/google/uuid"
	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
	"github.com/shawon-kanji/go-ride-utils/events"
	"github.com/shawon-kanji/go-ride-utils/httpheaders"
	"gorm.io/gorm"
)

const (
	requestStatusSearchStarted = "search_started"
	maxRequestBodyBytes        = 1 << 20
	apiPrefix                  = "/api/v1/cab"
	healthzRoute               = apiPrefix + "/healthz"
	requestRoute               = apiPrefix + "/request-cab"
	currentTripRoute           = apiPrefix + "/current-trip"
	fareEstimateRoute          = apiPrefix + "/fare-estimate"
)

var (
	activeTripRequestStatuses = []string{"search_started", "searching", "offered", "driver_accepted", "driver_rejected"}
	activeOngoingTripStatuses = []string{"assigned", "driver_arriving", "in_progress", "awaiting_payment"}
)

var (
	errFareNotFound    = errors.New("fare_not_found")
	errFareExpired     = errors.New("fare_expired")
	errFareAlreadyUsed = errors.New("fare_already_used")
)

type Server struct {
	cfg      config.Config
	db       *gorm.DB
	producer kafka.Producer
	http     *http.Server
}

type createCabRequestRequest struct {
	RiderID        string    `json:"rider_id"`
	FareID         string    `json:"fare_id"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
	RequestedAt    time.Time `json:"requested_at,omitempty"`
}

type fareEstimateRequest struct {
	RiderID        string  `json:"rider_id"`
	PickupLat      float64 `json:"pickup_lat"`
	PickupLng      float64 `json:"pickup_lng"`
	DropoffLat     float64 `json:"dropoff_lat"`
	DropoffLng     float64 `json:"dropoff_lng"`
	SearchRadiusKM float64 `json:"search_radius_km,omitempty"`
}

type fareEstimateResponse struct {
	FareID          string    `json:"fare_id"`
	CurrencyCode    string    `json:"currency_code"`
	BaseFare        float64   `json:"base_fare"`
	DistanceFare    float64   `json:"distance_fare"`
	TimeFare        float64   `json:"time_fare"`
	SurchargeTotal  float64   `json:"surcharge_total"`
	DiscountTotal   float64   `json:"discount_total"`
	SurgeMultiplier float64   `json:"surge_multiplier"`
	TotalFare       float64   `json:"total_fare"`
	PricingVersion  string    `json:"pricing_version"`
	LockedAt        time.Time `json:"locked_at"`
	ExpiresAt       time.Time `json:"expires_at,omitempty"`
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type createCabRequestResponse struct {
	Accepted       bool      `json:"accepted"`
	RequestID      string    `json:"request_id"`
	TripID         string    `json:"trip_id"`
	FareID         string    `json:"fare_id,omitempty"`
	Status         string    `json:"status"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
	EventID        string    `json:"event_id"`
	PublishedAt    time.Time `json:"published_at"`
	CurrencyCode   string    `json:"currency_code,omitempty"`
	EstimatedTotal float64   `json:"estimated_total_fare,omitempty"`
}

type requestBundle struct {
	request schemamodels.TripRequest
	fare    schemamodels.TripFare
}

type currentTripResponse struct {
	RiderID          string              `json:"rider_id"`
	HasActiveRequest bool                `json:"has_active_request"`
	HasOngoingTrip   bool                `json:"has_ongoing_trip"`
	TripRequest      *tripRequestPayload `json:"trip_request,omitempty"`
	OngoingTrip      *ongoingTripPayload `json:"ongoing_trip,omitempty"`
}

type tripRequestPayload struct {
	RequestID      string       `json:"request_id"`
	TripID         string       `json:"trip_id"`
	Status         string       `json:"status"`
	PickupLat      float64      `json:"pickup_lat"`
	PickupLng      float64      `json:"pickup_lng"`
	DropoffLat     float64      `json:"dropoff_lat"`
	DropoffLng     float64      `json:"dropoff_lng"`
	SearchRadiusKM float64      `json:"search_radius_km"`
	RequestedAt    time.Time    `json:"requested_at"`
	Fare           *farePayload `json:"fare,omitempty"`
}

type farePayload struct {
	FareID          string     `json:"fare_id"`
	CurrencyCode    string     `json:"currency_code"`
	BaseFare        float64    `json:"base_fare"`
	DistanceFare    float64    `json:"distance_fare"`
	TimeFare        float64    `json:"time_fare"`
	SurchargeTotal  float64    `json:"surcharge_total"`
	DiscountTotal   float64    `json:"discount_total"`
	SurgeMultiplier float64    `json:"surge_multiplier"`
	TotalFare       float64    `json:"total_fare"`
	PricingVersion  string     `json:"pricing_version"`
	LockedAt        time.Time  `json:"locked_at"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
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
	CancelledAt        *time.Time `json:"cancelled_at,omitempty"`
	FinalFare          *float64   `json:"final_fare,omitempty"`
	PaymentStatus      *string    `json:"payment_status,omitempty"`
	PaymentCollectedAt *time.Time `json:"payment_collected_at,omitempty"`
}

func NewServer(cfg config.Config, db *gorm.DB, producer kafka.Producer) *Server {
	server := &Server{
		cfg:      cfg,
		db:       db,
		producer: producer,
	}

	mux := http.NewServeMux()
	mux.HandleFunc(healthzRoute, server.handleHealthz)
	mux.HandleFunc(requestRoute, server.handleCreateCabRequest)
	mux.HandleFunc(currentTripRoute, server.handleCurrentTrip)
	mux.HandleFunc(fareEstimateRoute, server.handleFareEstimate)

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
			log.Printf("close kafka producer topic=%s: %v", s.cfg.KafkaTopic, err)
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("cab request handler listening addr=%s topic=%s", s.cfg.HTTPAddr, s.cfg.KafkaTopic)
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
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleCreateCabRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req createCabRequestRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	req.IdempotencyKey = firstNonEmpty(strings.TrimSpace(r.Header.Get(httpheaders.Idempotency)), strings.TrimSpace(req.IdempotencyKey))
	req.CorrelationID = firstNonEmpty(strings.TrimSpace(r.Header.Get(httpheaders.Correlation)), strings.TrimSpace(req.CorrelationID))

	if err := validateCreateCabRequestRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	now := time.Now().UTC()
	requestedAt := req.RequestedAt
	if requestedAt.IsZero() {
		requestedAt = now
	}
	if req.CorrelationID == "" {
		req.CorrelationID = uuid.NewString()
	}

	bundle, err := s.createOrLoadCabRequest(r.Context(), req, requestedAt, now)
	if err != nil {
		switch {
		case errors.Is(err, errFareNotFound):
			writeJSONError(w, http.StatusNotFound, "fare_not_found", "fare_id was not found")
		case errors.Is(err, errFareExpired):
			writeJSONError(w, http.StatusConflict, "fare_expired", "fare quote has expired; request a new fare estimate")
		case errors.Is(err, errFareAlreadyUsed):
			writeJSONError(w, http.StatusConflict, "fare_already_used", "fare_id has already been booked")
		default:
			log.Printf("persist cab request rider_id=%s: %v", req.RiderID, err)
			http.Error(w, "failed to create cab request", http.StatusInternalServerError)
		}
		return
	}

	event := s.newRideRequestedEvent(bundle, requestedAt, now)
	if err := s.producer.PublishRideRequested(r.Context(), event); err != nil {
		log.Printf("publish ride requested request_id=%s: %v", bundle.request.ID, err)
		http.Error(w, "failed to publish cab request", http.StatusBadGateway)
		return
	}

	response := createCabRequestResponse{
		Accepted:       true,
		RequestID:      bundle.request.ID.String(),
		TripID:         bundle.request.TripID.String(),
		Status:         bundle.request.Status,
		CorrelationID:  valueOrEmpty(bundle.request.CorrelationID),
		EventID:        event.EventID,
		PublishedAt:    event.PublishedAt,
		CurrencyCode:   bundle.fare.CurrencyCode,
		EstimatedTotal: bundle.fare.TotalFare,
	}
	if bundle.request.FareID != nil {
		response.FareID = bundle.request.FareID.String()
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(response)
	log.Printf("accepted cab request request_id=%s trip_id=%s status=%s", response.RequestID, response.TripID, response.Status)
}

func (s *Server) handleFareEstimate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req fareEstimateRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if err := validateFareEstimateRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	riderID, err := uuid.Parse(req.RiderID)
	if err != nil {
		http.Error(w, "rider_id must be a valid UUID", http.StatusBadRequest)
		return
	}

	if req.SearchRadiusKM <= 0 {
		req.SearchRadiusKM = s.cfg.DefaultSearchRadiusKM
	}

	now := time.Now().UTC()
	fare := s.buildFareEstimate(riderID, req, now)
	if err := s.db.WithContext(r.Context()).Create(&fare).Error; err != nil {
		log.Printf("persist fare estimate rider_id=%s: %v", req.RiderID, err)
		http.Error(w, "failed to create fare estimate", http.StatusInternalServerError)
		return
	}

	response := fareEstimateResponse{
		FareID:          fare.ID.String(),
		CurrencyCode:    fare.CurrencyCode,
		BaseFare:        fare.BaseFare,
		DistanceFare:    fare.DistanceFare,
		TimeFare:        fare.TimeFare,
		SurchargeTotal:  fare.SurchargeTotal,
		DiscountTotal:   fare.DiscountTotal,
		SurgeMultiplier: fare.SurgeMultiplier,
		TotalFare:       fare.TotalFare,
		PricingVersion:  fare.PricingVersion,
		LockedAt:        fare.LockedAt,
	}
	if fare.ExpiresAt != nil {
		response.ExpiresAt = *fare.ExpiresAt
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(response)
	log.Printf("created fare estimate fare_id=%s rider_id=%s", response.FareID, req.RiderID)
}

func (s *Server) handleCurrentTrip(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	riderIDValue := strings.TrimSpace(r.URL.Query().Get("rider_id"))
	if riderIDValue == "" {
		http.Error(w, "rider_id is required", http.StatusBadRequest)
		return
	}

	riderID, err := uuid.Parse(riderIDValue)
	if err != nil {
		http.Error(w, "rider_id must be a valid UUID", http.StatusBadRequest)
		return
	}

	tripRequest, fare, err := s.loadLatestActiveTripRequest(r.Context(), riderID)
	if err != nil {
		log.Printf("load active trip request rider_id=%s: %v", riderID, err)
		http.Error(w, "failed to load current trip state", http.StatusInternalServerError)
		return
	}

	ongoingTrip, err := s.loadLatestOngoingTrip(r.Context(), riderID)
	if err != nil {
		log.Printf("load ongoing trip rider_id=%s: %v", riderID, err)
		http.Error(w, "failed to load current trip state", http.StatusInternalServerError)
		return
	}

	response := currentTripResponse{
		RiderID:          riderID.String(),
		HasActiveRequest: tripRequest != nil,
		HasOngoingTrip:   ongoingTrip != nil,
	}

	if tripRequest != nil {
		requestPayload := &tripRequestPayload{
			RequestID:      tripRequest.ID.String(),
			TripID:         tripRequest.TripID.String(),
			Status:         tripRequest.Status,
			PickupLat:      tripRequest.PickupLat,
			PickupLng:      tripRequest.PickupLng,
			DropoffLat:     tripRequest.DropoffLat,
			DropoffLng:     tripRequest.DropoffLng,
			SearchRadiusKM: tripRequest.SearchRadiusKM,
			RequestedAt:    tripRequest.RequestedAt,
		}
		if fare != nil {
			requestPayload.Fare = &farePayload{
				FareID:          fare.ID.String(),
				CurrencyCode:    fare.CurrencyCode,
				BaseFare:        fare.BaseFare,
				DistanceFare:    fare.DistanceFare,
				TimeFare:        fare.TimeFare,
				SurchargeTotal:  fare.SurchargeTotal,
				DiscountTotal:   fare.DiscountTotal,
				SurgeMultiplier: fare.SurgeMultiplier,
				TotalFare:       fare.TotalFare,
				PricingVersion:  fare.PricingVersion,
				LockedAt:        fare.LockedAt,
				ExpiresAt:       fare.ExpiresAt,
			}
		}
		response.TripRequest = requestPayload
	}

	if ongoingTrip != nil {
		response.OngoingTrip = &ongoingTripPayload{
			TripRecordID:       ongoingTrip.ID.String(),
			RequestID:          ongoingTrip.RequestID.String(),
			TripID:             ongoingTrip.TripID.String(),
			DriverID:           ongoingTrip.DriverID.String(),
			Status:             ongoingTrip.Status,
			PickupLat:          ongoingTrip.PickupLat,
			PickupLng:          ongoingTrip.PickupLng,
			DropoffLat:         ongoingTrip.DropoffLat,
			DropoffLng:         ongoingTrip.DropoffLng,
			AssignedAt:         ongoingTrip.AssignedAt,
			StartedAt:          ongoingTrip.StartedAt,
			EndedAt:            ongoingTrip.EndedAt,
			CompletedAt:        ongoingTrip.CompletedAt,
			CancelledAt:        ongoingTrip.CancelledAt,
			FinalFare:          ongoingTrip.FinalFare,
			PaymentStatus:      ongoingTrip.PaymentStatus,
			PaymentCollectedAt: ongoingTrip.PaymentCollectedAt,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *Server) loadLatestActiveTripRequest(ctx context.Context, riderID uuid.UUID) (*schemamodels.TripRequest, *schemamodels.TripFare, error) {
	var request schemamodels.TripRequest
	err := s.db.WithContext(ctx).
		Where("rider_id = ? AND status IN ?", riderID, activeTripRequestStatuses).
		Order("requested_at DESC").
		Take(&request).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("query active trip request: %w", err)
	}

	if request.FareID == nil {
		return &request, nil, nil
	}

	var fare schemamodels.TripFare
	err = s.db.WithContext(ctx).Where("fare_id = ?", *request.FareID).Take(&fare).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return &request, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("query trip fare fare_id=%s: %w", request.FareID.String(), err)
	}

	return &request, &fare, nil
}

func (s *Server) loadLatestOngoingTrip(ctx context.Context, riderID uuid.UUID) (*schemamodels.OngoingTrip, error) {
	var ongoingTrip schemamodels.OngoingTrip
	err := s.db.WithContext(ctx).
		Where("rider_id = ? AND status IN ?", riderID, activeOngoingTripStatuses).
		Order("assigned_at DESC").
		Take(&ongoingTrip).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query ongoing trip: %w", err)
	}

	return &ongoingTrip, nil
}

func (s *Server) createOrLoadCabRequest(ctx context.Context, req createCabRequestRequest, requestedAt, now time.Time) (requestBundle, error) {
	riderID, err := uuid.Parse(req.RiderID)
	if err != nil {
		return requestBundle{}, fmt.Errorf("parse rider_id: %w", err)
	}

	fareID, err := uuid.Parse(req.FareID)
	if err != nil {
		return requestBundle{}, fmt.Errorf("parse fare_id: %w", err)
	}

	var bundle requestBundle
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if req.IdempotencyKey != "" {
			var existing schemamodels.TripRequest
			err := tx.Where("rider_id = ? AND idempotency_key = ?", riderID, req.IdempotencyKey).Take(&existing).Error
			if err == nil {
				fare, loadErr := loadTripFareByID(tx, *existing.FareID)
				if loadErr != nil {
					return loadErr
				}

				bundle = requestBundle{request: existing, fare: fare}
				return nil
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("check existing trip request: %w", err)
			}
		}

		var fare schemamodels.TripFare
		err := tx.Where("fare_id = ?", fareID).Take(&fare).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return errFareNotFound
		}
		if err != nil {
			return fmt.Errorf("load trip fare fare_id=%s: %w", fareID, err)
		}
		if fare.RiderID != riderID {
			return errFareNotFound
		}
		if fare.IsConsumed() {
			return errFareAlreadyUsed
		}
		if fare.IsExpired(now) {
			return errFareExpired
		}

		requestID := uuid.New()
		request := schemamodels.TripRequest{
			ID:              requestID,
			TripID:          uuid.New(),
			RiderID:         riderID,
			FareID:          &fareID,
			Status:          requestStatusSearchStarted,
			PickupLat:       fare.PickupLat,
			PickupLng:       fare.PickupLng,
			DropoffLat:      fare.DropoffLat,
			DropoffLng:      fare.DropoffLng,
			PickupGeohash:   fare.PickupGeohash,
			PickupS2CellID:  fare.PickupS2CellID,
			SearchRadiusKM:  fare.SearchRadiusKM,
			RequestedAt:     requestedAt,
			SearchStartedAt: &now,
			CorrelationID:   stringPtr(req.CorrelationID),
			IdempotencyKey:  stringPtr(req.IdempotencyKey),
			CreatedAt:       now,
			UpdatedAt:       now,
		}

		if err := tx.Create(&request).Error; err != nil {
			if req.IdempotencyKey != "" {
				var existing schemamodels.TripRequest
				if lookupErr := tx.Where("rider_id = ? AND idempotency_key = ?", riderID, req.IdempotencyKey).Take(&existing).Error; lookupErr == nil {
					existingFare, loadErr := loadTripFareByID(tx, *existing.FareID)
					if loadErr != nil {
						return loadErr
					}
					bundle = requestBundle{request: existing, fare: existingFare}
					return nil
				}
			}

			return fmt.Errorf("create trip request: %w", err)
		}

		result := tx.Model(&schemamodels.TripFare{}).
			Where("fare_id = ? AND request_id IS NULL", fareID).
			Updates(map[string]any{"request_id": requestID, "updated_at": now})
		if result.Error != nil {
			return fmt.Errorf("claim trip fare: %w", result.Error)
		}
		if result.RowsAffected == 0 {
			return errFareAlreadyUsed
		}

		fare.RequestID = &requestID
		bundle = requestBundle{request: request, fare: fare}
		return nil
	}); err != nil {
		return requestBundle{}, err
	}

	return bundle, nil
}

func (s *Server) buildFareEstimate(riderID uuid.UUID, req fareEstimateRequest, now time.Time) schemamodels.TripFare {
	distanceKM := haversineKM(req.PickupLat, req.PickupLng, req.DropoffLat, req.DropoffLng)
	estimatedMinutes := 0.0
	if s.cfg.FareAverageSpeedKPH > 0 {
		estimatedMinutes = (distanceKM / s.cfg.FareAverageSpeedKPH) * 60
	}

	baseFare := roundMoney(s.cfg.FareBaseAmount)
	distanceFare := roundMoney(distanceKM * s.cfg.FarePerKmAmount)
	timeFare := roundMoney(estimatedMinutes * s.cfg.FarePerMinuteAmount)
	surchargeTotal := 0.0
	discountTotal := 0.0
	surgeMultiplier := 1.0
	totalFare := roundMoney(baseFare + distanceFare + timeFare)
	if totalFare < s.cfg.FareMinimumAmount {
		totalFare = roundMoney(s.cfg.FareMinimumAmount)
	}

	expiresAt := now.Add(s.cfg.FareLockTTL)

	return schemamodels.TripFare{
		ID:              uuid.New(),
		RiderID:         riderID,
		PickupLat:       req.PickupLat,
		PickupLng:       req.PickupLng,
		DropoffLat:      req.DropoffLat,
		DropoffLng:      req.DropoffLng,
		PickupGeohash:   geohashFromLatLng(req.PickupLat, req.PickupLng),
		PickupS2CellID:  s2CellIDFromLatLng(req.PickupLat, req.PickupLng),
		SearchRadiusKM:  req.SearchRadiusKM,
		CurrencyCode:    s.cfg.FareCurrencyCode,
		BaseFare:        baseFare,
		DistanceFare:    distanceFare,
		TimeFare:        timeFare,
		SurchargeTotal:  surchargeTotal,
		DiscountTotal:   discountTotal,
		SurgeMultiplier: surgeMultiplier,
		TotalFare:       totalFare,
		PricingVersion:  s.cfg.FarePricingVersion,
		LockedAt:        now,
		ExpiresAt:       &expiresAt,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
}

func (s *Server) newRideRequestedEvent(bundle requestBundle, requestedAt, now time.Time) events.RideRequestedV1 {
	event := events.RideRequestedV1{
		RequestID:       bundle.request.ID.String(),
		TripID:          bundle.request.TripID.String(),
		RiderID:         bundle.request.RiderID.String(),
		Status:          bundle.request.Status,
		PickupLat:       bundle.request.PickupLat,
		PickupLng:       bundle.request.PickupLng,
		DropoffLat:      bundle.request.DropoffLat,
		DropoffLng:      bundle.request.DropoffLng,
		PickupGeohash:   bundle.request.PickupGeohash,
		PickupS2CellID:  bundle.request.PickupS2CellID,
		SearchRadiusKM:  bundle.request.SearchRadiusKM,
		RequestedAt:     requestedAt,
		CorrelationID:   valueOrEmpty(bundle.request.CorrelationID),
		EventID:         bundle.request.ID.String(),
		PublishedAt:     now,
		CurrencyCode:    bundle.fare.CurrencyCode,
		BaseFare:        bundle.fare.BaseFare,
		DistanceFare:    bundle.fare.DistanceFare,
		TimeFare:        bundle.fare.TimeFare,
		SurchargeTotal:  bundle.fare.SurchargeTotal,
		DiscountTotal:   bundle.fare.DiscountTotal,
		SurgeMultiplier: bundle.fare.SurgeMultiplier,
		TotalFare:       bundle.fare.TotalFare,
		PricingVersion:  bundle.fare.PricingVersion,
	}

	if bundle.request.FareID != nil {
		fareID := bundle.request.FareID.String()
		event.FareID = &fareID
	}
	if bundle.fare.LockedAt != (time.Time{}) {
		lockedAt := bundle.fare.LockedAt
		event.FareLockedAt = &lockedAt
	}
	if bundle.fare.ExpiresAt != nil {
		expiresAt := *bundle.fare.ExpiresAt
		event.FareExpiresAt = &expiresAt
	}

	return event
}

func validateCreateCabRequestRequest(req createCabRequestRequest) error {
	if _, err := uuid.Parse(req.RiderID); err != nil {
		return fmt.Errorf("rider_id must be a valid UUID")
	}
	if _, err := uuid.Parse(req.FareID); err != nil {
		return fmt.Errorf("fare_id must be a valid UUID")
	}
	if len(req.IdempotencyKey) > 128 {
		return fmt.Errorf("idempotency_key must be at most 128 characters")
	}
	if len(req.CorrelationID) > 128 {
		return fmt.Errorf("correlation_id must be at most 128 characters")
	}

	return nil
}

func validateFareEstimateRequest(req fareEstimateRequest) error {
	if _, err := uuid.Parse(req.RiderID); err != nil {
		return fmt.Errorf("rider_id must be a valid UUID")
	}
	if req.PickupLat < -90 || req.PickupLat > 90 {
		return fmt.Errorf("pickup_lat must be between -90 and 90")
	}
	if req.PickupLng < -180 || req.PickupLng > 180 {
		return fmt.Errorf("pickup_lng must be between -180 and 180")
	}
	if req.DropoffLat < -90 || req.DropoffLat > 90 {
		return fmt.Errorf("dropoff_lat must be between -90 and 90")
	}
	if req.DropoffLng < -180 || req.DropoffLng > 180 {
		return fmt.Errorf("dropoff_lng must be between -180 and 180")
	}
	if req.SearchRadiusKM < 0 {
		return fmt.Errorf("search_radius_km must be greater than or equal to 0")
	}

	return nil
}

func loadTripFareByID(tx *gorm.DB, fareID uuid.UUID) (schemamodels.TripFare, error) {
	var fare schemamodels.TripFare
	if err := tx.Where("fare_id = ?", fareID).Take(&fare).Error; err != nil {
		return schemamodels.TripFare{}, fmt.Errorf("load trip fare: %w", err)
	}
	return fare, nil
}

func writeJSONError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: code, Message: message})
}

func haversineKM(lat1, lng1, lat2, lng2 float64) float64 {
	const earthRadiusKM = 6371.0

	lat1Rad := degreesToRadians(lat1)
	lng1Rad := degreesToRadians(lng1)
	lat2Rad := degreesToRadians(lat2)
	lng2Rad := degreesToRadians(lng2)

	deltaLat := lat2Rad - lat1Rad
	deltaLng := lng2Rad - lng1Rad

	sinLat := math.Sin(deltaLat / 2)
	sinLng := math.Sin(deltaLng / 2)
	a := sinLat*sinLat + math.Cos(lat1Rad)*math.Cos(lat2Rad)*sinLng*sinLng
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadiusKM * c
}

func degreesToRadians(value float64) float64 {
	return value * math.Pi / 180
}

func roundMoney(value float64) float64 {
	return math.Round(value*100) / 100
}

func stringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
