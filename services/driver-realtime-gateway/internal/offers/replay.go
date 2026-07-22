package offers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go-ride-kafka-consumers/services/driver-realtime-gateway/internal/ws"

	"github.com/google/uuid"
	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
	"gorm.io/gorm"
)

type replayRow struct {
	JobOfferID    uuid.UUID `gorm:"column:job_offer_id"`
	RequestID     uuid.UUID `gorm:"column:request_id"`
	TripID        uuid.UUID `gorm:"column:trip_id"`
	OfferRank     int       `gorm:"column:offer_rank"`
	ExpiresAt     time.Time `gorm:"column:expires_at"`
	CorrelationID *string   `gorm:"column:correlation_id"`
	PickupLat     float64   `gorm:"column:pickup_lat"`
	PickupLng     float64   `gorm:"column:pickup_lng"`
	DropoffLat    float64   `gorm:"column:dropoff_lat"`
	DropoffLng    float64   `gorm:"column:dropoff_lng"`
	TotalFare     *float64  `gorm:"column:total_fare"`
	CurrencyCode  *string   `gorm:"column:currency_code"`
}

// Pickup/dropoff and the fare breakdown aren't denormalized onto
// driver_job_offers, so replay (the low-frequency reconnect path, unlike the
// Kafka-driven live push which carries them on the event) needs these joins.
// trip_fares is LEFT JOINed since trip_requests.fare_id is nullable in
// principle, even though in practice booking always sets it.
const replayQuery = `
SELECT doff.job_offer_id, doff.request_id, doff.trip_id, doff.offer_rank, doff.expires_at, doff.correlation_id,
       req.pickup_lat, req.pickup_lng, req.dropoff_lat, req.dropoff_lng,
       fare.total_fare, fare.currency_code
FROM driver_job_offers doff
JOIN trip_requests req ON req.request_id = doff.request_id
LEFT JOIN trip_fares fare ON fare.fare_id = req.fare_id
WHERE doff.driver_id = ? AND doff.status = ? AND doff.expires_at > ?
ORDER BY doff.offered_at ASC
LIMIT ?
`

// Replayer pushes any pending, non-expired offers to a driver on connect.
// First-connect and reconnect are handled identically: replay is idempotent
// since it only re-pushes rows still marked pending.
type Replayer struct {
	db             *gorm.DB
	store          *Store
	limit          int
	commissionRate float64
}

func NewReplayer(db *gorm.DB, store *Store, limit int, commissionRate float64) *Replayer {
	return &Replayer{db: db, store: store, limit: limit, commissionRate: commissionRate}
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

		var estimatedEarning float64
		var currencyCode string
		if row.TotalFare != nil {
			estimatedEarning = *row.TotalFare * (1 - r.commissionRate)
		}
		if row.CurrencyCode != nil {
			currencyCode = *row.CurrencyCode
		}

		message := ws.NewJobOfferMessage(ws.JobOfferParams{
			JobOfferID:       row.JobOfferID.String(),
			RequestID:        row.RequestID.String(),
			TripID:           row.TripID.String(),
			OfferRank:        row.OfferRank,
			PickupLat:        row.PickupLat,
			PickupLng:        row.PickupLng,
			DropoffLat:       row.DropoffLat,
			DropoffLng:       row.DropoffLng,
			EstimatedEarning: estimatedEarning,
			CurrencyCode:     currencyCode,
			ExpiresAt:        row.ExpiresAt,
			CorrelationID:    correlationID,
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
