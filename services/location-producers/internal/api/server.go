package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"go-ride-kafka-consumers/services/location-producers/internal/config"
	"go-ride-kafka-consumers/services/location-producers/internal/kafka"
	"go-ride-kafka-consumers/services/location-producers/pkg/events"

	"github.com/google/uuid"
)

type Server struct {
	cfg      config.Config
	producer kafka.Producer
	http     *http.Server
}

type updateLocationRequest struct {
	DriverID  string    `json:"driver_id"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	EventTime time.Time `json:"event_time"`
	Geohash   string    `json:"geohash,omitempty"`
	S2CellID  string    `json:"s2_cell_id,omitempty"`
	AccuracyM float64   `json:"accuracy_m,omitempty"`
	Source    string    `json:"source,omitempty"`
	EventID   string    `json:"event_id,omitempty"`
}

type updateLocationResponse struct {
	Accepted    bool      `json:"accepted"`
	EventID     string    `json:"event_id"`
	PublishedAt time.Time `json:"published_at"`
}

func NewServer(cfg config.Config, producer kafka.Producer) *Server {
	server := &Server{
		cfg:      cfg,
		producer: producer,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", server.handleHealthz)
	mux.HandleFunc("/update-location", server.handleUpdateLocation)

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
		log.Printf("location ingest listening addr=%s topic=%s", s.cfg.HTTPAddr, s.cfg.KafkaTopic)
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

func (s *Server) handleUpdateLocation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req updateLocationRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if err := validateUpdateLocationRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	now := time.Now().UTC()
	eventTime := req.EventTime
	if eventTime.IsZero() {
		eventTime = now
	}
	eventID := req.EventID
	if eventID == "" {
		eventID = uuid.NewString()
	}

	event := events.DriverLocationUpdatedV1{
		DriverID:    req.DriverID,
		Latitude:    req.Latitude,
		Longitude:   req.Longitude,
		EventTime:   eventTime,
		Geohash:     geohashFromLatLng(req.Latitude, req.Longitude),
		S2CellID:    s2CellIDFromLatLng(req.Latitude, req.Longitude),
		AccuracyM:   req.AccuracyM,
		Source:      req.Source,
		EventID:     eventID,
		PublishedAt: now,
	}

	if err := s.producer.PublishDriverLocation(r.Context(), event); err != nil {
		log.Printf("publish location event driver_id=%s: %v", req.DriverID, err)
		http.Error(w, "failed to publish location update", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(updateLocationResponse{
		Accepted:    true,
		EventID:     eventID,
		PublishedAt: now,
	})
}

func validateUpdateLocationRequest(req updateLocationRequest) error {
	if _, err := uuid.Parse(req.DriverID); err != nil {
		return fmt.Errorf("driver_id must be a valid UUID")
	}
	if req.Latitude < -90 || req.Latitude > 90 {
		return fmt.Errorf("latitude must be between -90 and 90")
	}
	if req.Longitude < -180 || req.Longitude > 180 {
		return fmt.Errorf("longitude must be between -180 and 180")
	}
	if req.AccuracyM < 0 {
		return fmt.Errorf("accuracy_m must be greater than or equal to 0")
	}

	return nil
}
