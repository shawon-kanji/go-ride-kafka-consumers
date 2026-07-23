package ws

import (
	"sync"

	"github.com/google/uuid"
)

// RiderHub is the local, single-instance presence registry for rider
// connections: rider_id -> device_id -> live connection. Same shape as Hub
// (driver presence) — kept separate rather than parameterized since driver
// and rider connections diverge (drivers ack, riders don't).
type RiderHub struct {
	mu      sync.RWMutex
	byRider map[uuid.UUID]map[string]*RiderConnection
}

func NewRiderHub() *RiderHub {
	return &RiderHub{byRider: make(map[uuid.UUID]map[string]*RiderConnection)}
}

func (h *RiderHub) Register(conn *RiderConnection) {
	h.mu.Lock()
	defer h.mu.Unlock()

	devices, ok := h.byRider[conn.RiderID]
	if !ok {
		devices = make(map[string]*RiderConnection)
		h.byRider[conn.RiderID] = devices
	}
	if existing, ok := devices[conn.DeviceID]; ok {
		existing.Close()
	}
	devices[conn.DeviceID] = conn
}

func (h *RiderHub) Unregister(conn *RiderConnection) {
	h.mu.Lock()
	defer h.mu.Unlock()

	devices, ok := h.byRider[conn.RiderID]
	if !ok {
		return
	}
	if devices[conn.DeviceID] == conn {
		delete(devices, conn.DeviceID)
	}
	if len(devices) == 0 {
		delete(h.byRider, conn.RiderID)
	}
}

// ConnectionsForRider returns this instance's live connections for a rider
// (across devices). Empty means this instance doesn't hold that rider's
// connection right now — that's a routing no-op, not an error.
func (h *RiderHub) ConnectionsForRider(riderID uuid.UUID) []*RiderConnection {
	h.mu.RLock()
	defer h.mu.RUnlock()

	devices, ok := h.byRider[riderID]
	if !ok {
		return nil
	}
	conns := make([]*RiderConnection, 0, len(devices))
	for _, conn := range devices {
		conns = append(conns, conn)
	}
	return conns
}
