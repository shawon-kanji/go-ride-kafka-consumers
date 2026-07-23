package ws

import (
	"sync"

	"github.com/google/uuid"
)

// Hub is the local, single-instance presence registry: driver_id -> device_id
// -> live connection. It holds no cross-instance state — routing a message
// from another gateway instance to whichever instance actually holds a
// driver's connection is Redis pub/sub's job (internal/presence), not the
// Hub's.
type Hub struct {
	mu       sync.RWMutex
	byDriver map[uuid.UUID]map[string]*Connection
}

func NewHub() *Hub {
	return &Hub{byDriver: make(map[uuid.UUID]map[string]*Connection)}
}

func (h *Hub) Register(conn *Connection) {
	h.mu.Lock()
	defer h.mu.Unlock()

	devices, ok := h.byDriver[conn.DriverID]
	if !ok {
		devices = make(map[string]*Connection)
		h.byDriver[conn.DriverID] = devices
	}
	if existing, ok := devices[conn.DeviceID]; ok {
		existing.Close()
	}
	devices[conn.DeviceID] = conn
}

func (h *Hub) Unregister(conn *Connection) {
	h.mu.Lock()
	defer h.mu.Unlock()

	devices, ok := h.byDriver[conn.DriverID]
	if !ok {
		return
	}
	if devices[conn.DeviceID] == conn {
		delete(devices, conn.DeviceID)
	}
	if len(devices) == 0 {
		delete(h.byDriver, conn.DriverID)
	}
}

// ConnectionsForDriver returns this instance's live connections for a driver
// (across devices). Empty means this instance doesn't hold that driver's
// connection right now — that's a routing no-op, not an error.
func (h *Hub) ConnectionsForDriver(driverID uuid.UUID) []*Connection {
	h.mu.RLock()
	defer h.mu.RUnlock()

	devices, ok := h.byDriver[driverID]
	if !ok {
		return nil
	}
	conns := make([]*Connection, 0, len(devices))
	for _, conn := range devices {
		conns = append(conns, conn)
	}
	return conns
}
