package ws

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const writeWait = 10 * time.Second

// Connection wraps one driver device's live websocket, owning its own
// read/write pumps. Outbound writes go through a buffered channel so any
// number of goroutines (hub broadcasts, replay, ack responses) can push to it
// without touching the underlying *websocket.Conn directly (gorilla only
// allows one writer at a time).
type Connection struct {
	DriverID uuid.UUID
	DeviceID string

	ctx    context.Context
	conn   *websocket.Conn
	send   chan []byte
	onAck  func(ctx context.Context, ack AckMessage)
	closed chan struct{}
}

// NewConnection starts the connection's read/write pumps. ctx is the
// connection's background lifetime context (not the originating HTTP
// request's context, which is canceled once the upgrade handler returns) —
// it's what onAck callbacks receive for their DB writes.
func NewConnection(ctx context.Context, driverID uuid.UUID, deviceID string, conn *websocket.Conn, pingInterval, pongWait time.Duration, onAck func(ctx context.Context, ack AckMessage)) *Connection {
	c := &Connection{
		DriverID: driverID,
		DeviceID: deviceID,
		ctx:      ctx,
		conn:     conn,
		send:     make(chan []byte, 32),
		onAck:    onAck,
		closed:   make(chan struct{}),
	}

	go c.writePump(pingInterval)
	go c.readPump(pongWait)

	return c
}

// Send enqueues a message for delivery; returns false if the connection's
// outbound buffer is full or already closed (caller treats this as a
// "driver offline" signal, not a retryable error).
func (c *Connection) Send(payload []byte) bool {
	select {
	case c.send <- payload:
		return true
	case <-c.closed:
		return false
	default:
		return false
	}
}

// Done reports when the connection has closed, so the server can unregister
// it from the hub.
func (c *Connection) Done() <-chan struct{} {
	return c.closed
}

func (c *Connection) Close() {
	select {
	case <-c.closed:
	default:
		close(c.closed)
		_ = c.conn.Close()
	}
}

func (c *Connection) writePump(pingInterval time.Duration) {
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

func (c *Connection) readPump(pongWait time.Duration) {
	defer c.Close()

	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, payload, err := c.conn.ReadMessage()
		if err != nil {
			return
		}

		var ack AckMessage
		if err := json.Unmarshal(payload, &ack); err != nil {
			log.Printf("driver_realtime_gateway: discard unparseable ws frame driver_id=%s device_id=%s: %v", c.DriverID, c.DeviceID, err)
			continue
		}
		if ack.Type != "ack" {
			continue
		}
		if c.onAck != nil {
			c.onAck(c.ctx, ack)
		}
	}
}
