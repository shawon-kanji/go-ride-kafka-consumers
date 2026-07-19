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
	"go-ride-kafka-consumers/services/cab-request-handler/pkg/events"

	"github.com/google/uuid"
	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
	"gorm.io/gorm"
)

const (
	requestStatusSearchStarted = "search_started"
	maxRequestBodyBytes        = 1 << 20
	requestRoute               = "/request-cab"
)

type Server struct {
	cfg      config.Config
	db       *gorm.DB
	producer kafka.Producer
	http     *http.Server
}

type createCabRequestRequest struct {
	RiderID        string    `json:"rider_id"`
	PickupLat      float64   `json:"pickup_lat"`
	PickupLng      float64   `json:"pickup_lng"`
	DropoffLat     float64   `json:"dropoff_lat"`
	DropoffLng     float64   `json:"dropoff_lng"`
	SearchRadiusKM float64   `json:"search_radius_km,omitempty"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
	RequestedAt    time.Time `json:"requested_at,omitempty"`
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

func NewServer(cfg config.Config, db *gorm.DB, producer kafka.Producer) *Server {
	server := &Server{
		cfg:      cfg,
		db:       db,
		producer: producer,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", server.handleHealthz)
	mux.HandleFunc(requestRoute, server.handleCreateCabRequest)

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

	req.IdempotencyKey = firstNonEmpty(strings.TrimSpace(r.Header.Get("Idempotency-Key")), strings.TrimSpace(req.IdempotencyKey))
	req.CorrelationID = firstNonEmpty(strings.TrimSpace(r.Header.Get("X-Correlation-ID")), strings.TrimSpace(req.CorrelationID))

	if err := validateCreateCabRequestRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	now := time.Now().UTC()
	requestedAt := req.RequestedAt
	if requestedAt.IsZero() {
		requestedAt = now
	}
	if req.SearchRadiusKM <= 0 {
		req.SearchRadiusKM = s.cfg.DefaultSearchRadiusKM
	}
	if req.CorrelationID == "" {
		req.CorrelationID = uuid.NewString()
	}

	bundle, err := s.createOrLoadCabRequest(r.Context(), req, requestedAt, now)
	if err != nil {
		log.Printf("persist cab request rider_id=%s: %v", req.RiderID, err)
		http.Error(w, "failed to create cab request", http.StatusInternalServerError)
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

func (s *Server) createOrLoadCabRequest(ctx context.Context, req createCabRequestRequest, requestedAt, now time.Time) (requestBundle, error) {
	riderID, err := uuid.Parse(req.RiderID)
	if err != nil {
		return requestBundle{}, fmt.Errorf("parse rider_id: %w", err)
	}

	searchRadiusKM := req.SearchRadiusKM
	if searchRadiusKM <= 0 {
		searchRadiusKM = s.cfg.DefaultSearchRadiusKM
	}

	requestID := uuid.New()
	tripID := uuid.New()
	request := schemamodels.TripRequest{
		ID:              requestID,
		TripID:          tripID,
		RiderID:         riderID,
		Status:          requestStatusSearchStarted,
		PickupLat:       req.PickupLat,
		PickupLng:       req.PickupLng,
		DropoffLat:      req.DropoffLat,
		DropoffLng:      req.DropoffLng,
		PickupGeohash:   geohashFromLatLng(req.PickupLat, req.PickupLng),
		PickupS2CellID:  s2CellIDFromLatLng(req.PickupLat, req.PickupLng),
		SearchRadiusKM:  searchRadiusKM,
		RequestedAt:     requestedAt,
		SearchStartedAt: &now,
		CorrelationID:   stringPtr(req.CorrelationID),
		IdempotencyKey:  stringPtr(req.IdempotencyKey),
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	var bundle requestBundle
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if req.IdempotencyKey != "" {
			var existing schemamodels.TripRequest
			err := tx.Where("rider_id = ? AND idempotency_key = ?", riderID, req.IdempotencyKey).Take(&existing).Error
			if err == nil {
				fare, loadErr := loadTripFareByRequestID(tx, existing.ID)
				if loadErr != nil {
					return loadErr
				}

				bundle = requestBundle{request: existing, fare: fare}
				return nil
			}
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("check existing trip request: %w", err)
			}
		}

		result := tx.Create(&request)
		if result.Error != nil {
			if req.IdempotencyKey != "" {
				var existing schemamodels.TripRequest
				if err := tx.Where("rider_id = ? AND idempotency_key = ?", riderID, req.IdempotencyKey).Take(&existing).Error; err == nil {
					fare, loadErr := loadTripFareByRequestID(tx, existing.ID)
					if loadErr != nil {
						return loadErr
					}
					bundle = requestBundle{request: existing, fare: fare}
					return nil
				}
			}

			return fmt.Errorf("create trip request: %w", result.Error)
		}

		fare := s.buildTripFare(request.ID, req, now)
		if err := tx.Create(&fare).Error; err != nil {
			return fmt.Errorf("create trip fare: %w", err)
		}

		fareID := fare.ID
		if err := tx.Model(&request).Where("request_id = ?", request.ID).Updates(map[string]any{
			"fare_id":    fareID,
			"updated_at": now,
		}).Error; err != nil {
			return fmt.Errorf("attach fare to trip request: %w", err)
		}

		request.FareID = &fareID
		bundle = requestBundle{request: request, fare: fare}
		return nil
	}); err != nil {
		return requestBundle{}, err
	}

	return bundle, nil
}

func (s *Server) buildTripFare(requestID uuid.UUID, req createCabRequestRequest, now time.Time) schemamodels.TripFare {
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
		RequestID:       requestID,
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
	if len(req.IdempotencyKey) > 128 {
		return fmt.Errorf("idempotency_key must be at most 128 characters")
	}
	if len(req.CorrelationID) > 128 {
		return fmt.Errorf("correlation_id must be at most 128 characters")
	}

	return nil
}

func loadTripFareByRequestID(tx *gorm.DB, requestID uuid.UUID) (schemamodels.TripFare, error) {
	var fare schemamodels.TripFare
	if err := tx.Where("request_id = ?", requestID).Take(&fare).Error; err != nil {
		return schemamodels.TripFare{}, fmt.Errorf("load trip fare: %w", err)
	}
	return fare, nil
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
