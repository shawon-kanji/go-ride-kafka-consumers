package location

import (
	"time"

	"github.com/google/uuid"
)

// DriverLocation is the domain shape used by workers for persistence operations.
type DriverLocation struct {
	ID         uuid.UUID
	DriverID   uuid.UUID
	Latitude   float64
	Longitude  float64
	Geohash    string
	S2CellID   string
	RecordedAt time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
