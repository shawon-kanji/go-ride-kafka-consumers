package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const rateTripRoute = apiPrefix + "/trips/{ongoing_trip_id}/rate"

var (
	errRatingTripNotFound     = errors.New("trip_not_found")
	errRatingTripForbidden    = errors.New("trip_forbidden")
	errRatingTripNotCompleted = errors.New("trip_not_completed")
	errRatingAlreadyRated     = errors.New("trip_already_rated")
)

type rateTripRequest struct {
	Rating  int    `json:"rating"`
	Comment string `json:"comment,omitempty"`
}

type rateTripResponse struct {
	Accepted            bool    `json:"accepted"`
	OngoingTripID       string  `json:"ongoing_trip_id"`
	Rating              int     `json:"rating"`
	DriverRatingAverage float64 `json:"driver_rating_average"`
	DriverRatingCount   int     `json:"driver_rating_count"`
}

// handleRateTrip lets a rider rate the driver of a completed trip they were
// on — B7, rider-rates-driver only (the design has no driver-rates-rider
// screen). One rating per trip; a second attempt is a 409, not silently
// overwritten.
func (s *Server) handleRateTrip(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	riderID, ok := s.authenticateRider(w, r)
	if !ok {
		return
	}

	ongoingTripID, err := uuid.Parse(r.PathValue("ongoing_trip_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_ongoing_trip_id", "ongoing_trip_id must be a valid UUID")
		return
	}

	var req rateTripRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}
	if req.Rating < 1 || req.Rating > 5 {
		writeJSONError(w, http.StatusBadRequest, "invalid_rating", "rating must be an integer from 1 to 5")
		return
	}

	driverID, newAverage, newCount, err := s.rateTrip(r.Context(), ongoingTripID, riderID, req.Rating, req.Comment)
	if err != nil {
		switch {
		case errors.Is(err, errRatingTripNotFound):
			writeJSONError(w, http.StatusNotFound, "trip_not_found", "ongoing trip was not found")
		case errors.Is(err, errRatingTripForbidden):
			writeJSONError(w, http.StatusForbidden, "trip_forbidden", "this trip does not belong to you")
		case errors.Is(err, errRatingTripNotCompleted):
			writeJSONError(w, http.StatusConflict, "trip_not_completed", "only a completed trip can be rated")
		case errors.Is(err, errRatingAlreadyRated):
			writeJSONError(w, http.StatusConflict, "trip_already_rated", "this trip has already been rated")
		default:
			log.Printf("rate trip ongoing_trip_id=%s rider_id=%s: %v", ongoingTripID, riderID, err)
			writeJSONError(w, http.StatusInternalServerError, "internal_error", "failed to save rating")
		}
		return
	}

	response := rateTripResponse{
		Accepted:            true,
		OngoingTripID:       ongoingTripID.String(),
		Rating:              req.Rating,
		DriverRatingAverage: newAverage,
		DriverRatingCount:   newCount,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
	log.Printf("trip rated ongoing_trip_id=%s driver_id=%s rating=%d", ongoingTripID, driverID, req.Rating)
}

// rateTrip locks the ongoing trip and the driver's aggregate row in one
// transaction: the ongoing-trip lock serializes concurrent rating attempts
// on the same trip (so the plain existence check against trip_ratings right
// after it is race-free without its own lock), and the driver-row lock
// prevents a lost update if the same driver is rated on two different trips
// at once.
func (s *Server) rateTrip(ctx context.Context, ongoingTripID, riderID uuid.UUID, rating int, comment string) (driverID uuid.UUID, newAverage float64, newCount int, err error) {
	now := time.Now().UTC()

	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var trip schemamodels.OngoingTrip
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", ongoingTripID).
			Take(&trip).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errRatingTripNotFound
			}
			return fmt.Errorf("lock ongoing trip id=%s: %w", ongoingTripID, err)
		}

		if trip.RiderID != riderID {
			return errRatingTripForbidden
		}
		if trip.Status != schemamodels.OngoingTripStatusCompleted {
			return errRatingTripNotCompleted
		}

		var existing schemamodels.TripRating
		err := tx.Where("ongoing_trip_id = ?", ongoingTripID).Take(&existing).Error
		if err == nil {
			return errRatingAlreadyRated
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check existing rating ongoing_trip_id=%s: %w", ongoingTripID, err)
		}

		var commentPtr *string
		if comment != "" {
			commentPtr = &comment
		}
		tripRating := schemamodels.TripRating{
			ID:            uuid.New(),
			OngoingTripID: ongoingTripID,
			RiderID:       riderID,
			DriverID:      trip.DriverID,
			Rating:        rating,
			Comment:       commentPtr,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		if err := tx.Create(&tripRating).Error; err != nil {
			return fmt.Errorf("create trip rating ongoing_trip_id=%s: %w", ongoingTripID, err)
		}

		var driver schemamodels.Driver
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", trip.DriverID).
			Take(&driver).Error; err != nil {
			return fmt.Errorf("lock driver id=%s: %w", trip.DriverID, err)
		}

		priorAverage := 0.0
		if driver.RatingAverage != nil {
			priorAverage = *driver.RatingAverage
		}
		newCount = driver.RatingCount + 1
		newAverage = roundMoney((priorAverage*float64(driver.RatingCount) + float64(rating)) / float64(newCount))
		driverID = trip.DriverID

		if err := tx.Model(&schemamodels.Driver{}).Where("id = ?", trip.DriverID).Updates(map[string]any{
			"rating_average": newAverage,
			"rating_count":   newCount,
			"updated_at":     now,
		}).Error; err != nil {
			return fmt.Errorf("update driver rating aggregate driver_id=%s: %w", trip.DriverID, err)
		}

		return nil
	})
	if err != nil {
		return uuid.UUID{}, 0, 0, err
	}

	return driverID, newAverage, newCount, nil
}
