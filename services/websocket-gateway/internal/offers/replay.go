package offers

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/ws"

	"github.com/google/uuid"
	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
	"gorm.io/gorm"
)

type replayRow struct {
	JobOfferID          uuid.UUID `gorm:"column:job_offer_id"`
	RequestID           uuid.UUID `gorm:"column:request_id"`
	TripID              uuid.UUID `gorm:"column:trip_id"`
	OfferRank           int       `gorm:"column:offer_rank"`
	ExpiresAt           time.Time `gorm:"column:expires_at"`
	CorrelationID       *string   `gorm:"column:correlation_id"`
	RiderFirstName      *string   `gorm:"column:rider_first_name"`
	RiderLastName       *string   `gorm:"column:rider_last_name"`
	PickupLat           float64   `gorm:"column:pickup_lat"`
	PickupLng           float64   `gorm:"column:pickup_lng"`
	DropoffLat          float64   `gorm:"column:dropoff_lat"`
	DropoffLng          float64   `gorm:"column:dropoff_lng"`
	TripDistanceKM      *float64  `gorm:"column:route_distance_km"`
	TripDurationMinutes *float64  `gorm:"column:route_duration_minutes"`
	TotalFare           *float64  `gorm:"column:total_fare"`
	CurrencyCode        *string   `gorm:"column:currency_code"`
	DriverLat           *float64  `gorm:"column:driver_lat"`
	DriverLng           *float64  `gorm:"column:driver_lng"`
}

// Pickup/dropoff, the fare breakdown and the rider's name aren't denormalized
// onto driver_job_offers, so replay (the low-frequency reconnect path,
// unlike the Kafka-driven live push which carries them on the event) needs
// these joins. trip_fares is LEFT JOINed since trip_requests.fare_id is
// nullable in principle, even though in practice booking always sets it.
// driver_locations is LEFT JOINed on the same driver_id replay is already
// scoped to, so this is a single-row lookup, not a fan-out.
const replayQuery = `
SELECT doff.job_offer_id, doff.request_id, doff.trip_id, doff.offer_rank, doff.expires_at, doff.correlation_id,
       u.first_name AS rider_first_name, u.last_name AS rider_last_name,
       req.pickup_lat, req.pickup_lng, req.dropoff_lat, req.dropoff_lng,
       fare.route_distance_km, fare.route_duration_minutes, fare.total_fare, fare.currency_code,
       dl.latitude AS driver_lat, dl.longitude AS driver_lng
FROM driver_job_offers doff
JOIN trip_requests req ON req.request_id = doff.request_id
LEFT JOIN trip_fares fare ON fare.fare_id = req.fare_id
LEFT JOIN users u ON u.id = req.rider_id
LEFT JOIN driver_locations dl ON dl.driver_id = doff.driver_id
WHERE doff.driver_id = ? AND doff.status = ? AND doff.expires_at > ?
ORDER BY doff.offered_at ASC
LIMIT ?
`

// Replayer pushes any pending, non-expired offers to a driver on connect.
// First-connect and reconnect are handled identically: replay is idempotent
// since it only re-pushes rows still marked pending.
//
// Unlike the live push (which carries the pickup distance/ETA that dispatch
// computed once, at offer-creation time), replay recomputes it fresh from
// the driver's current driver_locations row — more accurate for a
// reconnect, which by definition happens some time after the offer was
// created, and free of any staleness tradeoff since it's a single extra
// joined column, not a new query.
type Replayer struct {
	db             *gorm.DB
	store          *Store
	limit          int
	commissionRate float64
	avgSpeedKPH    float64
}

func NewReplayer(db *gorm.DB, store *Store, limit int, commissionRate, avgSpeedKPH float64) *Replayer {
	return &Replayer{db: db, store: store, limit: limit, commissionRate: commissionRate, avgSpeedKPH: avgSpeedKPH}
}

func (r *Replayer) Replay(ctx context.Context, conn *ws.Connection) error {
	var rows []replayRow
	err := r.db.WithContext(ctx).
		Raw(replayQuery, conn.DriverID, schemamodels.DriverJobOfferStatusPending, time.Now().UTC(), r.limit).
		Scan(&rows).Error
	if err != nil {
		return fmt.Errorf("query replay offers driver_id=%s: %w", conn.DriverID, err)
	}

	for _, row := range rows {
		correlationID := ""
		if row.CorrelationID != nil {
			correlationID = *row.CorrelationID
		}

		riderName := ""
		if row.RiderFirstName != nil || row.RiderLastName != nil {
			riderName = strings.TrimSpace(deref(row.RiderFirstName) + " " + deref(row.RiderLastName))
		}

		var estimatedEarning float64
		var currencyCode string
		if row.TotalFare != nil {
			estimatedEarning = *row.TotalFare * (1 - r.commissionRate)
		}
		if row.CurrencyCode != nil {
			currencyCode = *row.CurrencyCode
		}

		var pickupDistanceKM, pickupETAMinutes float64
		if row.DriverLat != nil && row.DriverLng != nil {
			pickupDistanceKM = haversineKM(*row.DriverLat, *row.DriverLng, row.PickupLat, row.PickupLng)
			pickupETAMinutes = (pickupDistanceKM / r.avgSpeedKPH) * 60
		}

		message := ws.NewJobOfferMessage(ws.JobOfferParams{
			JobOfferID:          row.JobOfferID.String(),
			RequestID:           row.RequestID.String(),
			TripID:              row.TripID.String(),
			OfferRank:           row.OfferRank,
			RiderName:           riderName,
			PickupLat:           row.PickupLat,
			PickupLng:           row.PickupLng,
			DropoffLat:          row.DropoffLat,
			DropoffLng:          row.DropoffLng,
			TripDistanceKM:      deref(row.TripDistanceKM),
			TripDurationMinutes: deref(row.TripDurationMinutes),
			PickupDistanceKM:    pickupDistanceKM,
			PickupETAMinutes:    pickupETAMinutes,
			EstimatedEarning:    estimatedEarning,
			CurrencyCode:        currencyCode,
			ExpiresAt:           row.ExpiresAt,
			CorrelationID:       correlationID,
		})

		payload, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal replay offer job_offer_id=%s: %w", row.JobOfferID, err)
		}

		if !conn.Send(payload) {
			continue
		}
		if err := r.store.MarkSent(ctx, row.JobOfferID); err != nil {
			return err
		}
	}

	return nil
}

// deref returns the zero value for a nil pointer instead of panicking.
func deref[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

// haversineKM is deliberately duplicated rather than shared — same small-
// helper-per-package convention this codebase already follows (see
// tracking.haversineKM).
func haversineKM(lat1, lng1, lat2, lng2 float64) float64 {
	const earthRadiusKM = 6371.0

	lat1Rad := lat1 * math.Pi / 180
	lng1Rad := lng1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180
	lng2Rad := lng2 * math.Pi / 180

	deltaLat := lat2Rad - lat1Rad
	deltaLng := lng2Rad - lng1Rad

	sinLat := math.Sin(deltaLat / 2)
	sinLng := math.Sin(deltaLng / 2)
	a := sinLat*sinLat + math.Cos(lat1Rad)*math.Cos(lat2Rad)*sinLng*sinLng
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadiusKM * c
}
