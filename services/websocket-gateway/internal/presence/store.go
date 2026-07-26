package presence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ActiveTrip is the driver->rider routing target recorded once a driver wins
// a job offer, so the (fleet-wide, unauthenticated, continuous) location
// firehose can be filtered down to only drivers currently mid-trip before
// anything gets broadcast cross-instance.
type ActiveTrip struct {
	RiderID       string `json:"rider_id"`
	TripID        string `json:"trip_id"`
	OngoingTripID string `json:"ongoing_trip_id"`
	RequestID     string `json:"request_id"`
}

// Store is the KV counterpart to Bus: same Redis server, but a distinct
// concern (lookup, not fan-out), so it gets its own client rather than
// bolting SET/GET onto Bus's pub/sub-only API.
type Store struct {
	client *redis.Client
	ttl    time.Duration
}

func NewStore(addr, password string, db int, ttl time.Duration) *Store {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	return &Store{client: client, ttl: ttl}
}

func activeTripKey(driverID string) string {
	return "websocket-gateway:active-trip:" + driverID
}

// SetActiveTrip records driverID as currently working trip until the TTL
// elapses. There is no trip-completion/cancellation event yet to clear this
// early, so the TTL is the only expiry mechanism for now — a future
// trip-start/trip-cancelled phase should call ClearActiveTrip instead of
// waiting it out.
func (s *Store) SetActiveTrip(ctx context.Context, driverID string, trip ActiveTrip) error {
	payload, err := json.Marshal(trip)
	if err != nil {
		return fmt.Errorf("marshal active trip driver_id=%s: %w", driverID, err)
	}

	if err := s.client.Set(ctx, activeTripKey(driverID), payload, s.ttl).Err(); err != nil {
		return fmt.Errorf("set active trip driver_id=%s: %w", driverID, err)
	}
	return nil
}

// GetActiveTrip reports ok=false, err=nil on a Redis miss — that's the
// expected, majority-case outcome for a driver not currently on a trip, not
// an error condition.
func (s *Store) GetActiveTrip(ctx context.Context, driverID string) (ActiveTrip, bool, error) {
	payload, err := s.client.Get(ctx, activeTripKey(driverID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return ActiveTrip{}, false, nil
	}
	if err != nil {
		return ActiveTrip{}, false, fmt.Errorf("get active trip driver_id=%s: %w", driverID, err)
	}

	var trip ActiveTrip
	if err := json.Unmarshal(payload, &trip); err != nil {
		return ActiveTrip{}, false, fmt.Errorf("unmarshal active trip driver_id=%s: %w", driverID, err)
	}
	return trip, true, nil
}

// ClearActiveTrip is unused today — it's the hook a future trip-start/
// trip-cancelled phase calls to stop the location stream before the TTL
// would otherwise expire it.
func (s *Store) ClearActiveTrip(ctx context.Context, driverID string) error {
	if err := s.client.Del(ctx, activeTripKey(driverID)).Err(); err != nil {
		return fmt.Errorf("clear active trip driver_id=%s: %w", driverID, err)
	}
	return nil
}

func (s *Store) Close() error {
	return s.client.Close()
}
