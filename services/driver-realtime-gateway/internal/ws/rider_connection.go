package ws

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// RiderConnection wraps one rider device's live websocket. Unlike the driver
// Connection, this is fire-and-forget push-only for this slice: riders don't
// send an ack frame back, so readPump only exists to service ping/pong and
// notice disconnects.
type RiderConnection struct {
	RiderID  uuid.UUID
	DeviceID string

	conn   *websocket.Conn
	send   chan []byte
	closed chan struct{}
}

func NewRiderConnection(ctx context.Context, riderID uuid.UUID, deviceID string, conn *websocket.Conn, pingInterval, pongWait time.Duration) *RiderConnection {
	c := &RiderConnection{
		RiderID:  riderID,
		DeviceID: deviceID,
		conn:     conn,
		send:     make(chan []byte, 32),
		closed:   make(chan struct{}),
	}

	go c.writePump(pingInterval)
	go c.readPump(pongWait)

	return c
}

// Send enqueues a message for delivery; returns false if the connection's
// outbound buffer is full or already closed (caller treats this as a
// "rider offline" signal, not a retryable error).
func (c *RiderConnection) Send(payload []byte) bool {
	select {
	case c.send <- payload:
		return true
	case <-c.closed:
		return false
	default:
		return false
	}
}

func (c *RiderConnection) Done() <-chan struct{} {
	return c.closed
}

func (c *RiderConnection) Close() {
	select {
	case <-c.closed:
	default:
		close(c.closed)
		_ = c.conn.Close()
	}
}

func (c *RiderConnection) writePump(pingInterval time.Duration) {
	ticker := time.NewTicker(pingInterval)
	defer func() {
		ticker.Stop()
		c.Close()
	}()

	for {
		select {
		case payload, ok := <-c.send:
			if !ok {
				return
			}
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-c.closed:
			return
		}
	}
}

func (c *RiderConnection) readPump(pongWait time.Duration) {
	defer c.Close()

	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}
