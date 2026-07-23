package ws

import (
	"context"
	"log"
	"net/http"
	"time"

	"go-ride-kafka-consumers/services/driver-realtime-gateway/internal/auth"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	// Driver mobile clients aren't browser pages subject to CSRF-via-origin
	// concerns the way a rider web dashboard might be; auth is the JWT below.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Server handles the driver-facing WebSocket upgrade endpoint. onConnect and
// onAck are injected by bootstrap (wired to internal/offers) rather than
// imported directly, since internal/offers imports internal/ws and a direct
// import back would cycle.
type Server struct {
	hub          *Hub
	riderHub     *RiderHub
	verifier     *auth.Verifier
	pingInterval time.Duration
	pongWait     time.Duration
	onConnect    func(ctx context.Context, conn *Connection)
	onAck        func(ctx context.Context, ack AckMessage)
}

func NewServer(hub *Hub, riderHub *RiderHub, verifier *auth.Verifier, pingInterval, pongWait time.Duration, onConnect func(ctx context.Context, conn *Connection), onAck func(ctx context.Context, ack AckMessage)) *Server {
	return &Server{
		hub:          hub,
		riderHub:     riderHub,
		verifier:     verifier,
		pingInterval: pingInterval,
		pongWait:     pongWait,
		onConnect:    onConnect,
		onAck:        onAck,
	}
}

func (s *Server) Routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /ws/driver", s.handleDriverWS)
	mux.HandleFunc("GET /ws/rider", s.handleRiderWS)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// handleDriverWS upgrades GET /ws/driver?token=<jwt>&device_id=<id>. Token is
// a query param (not a header) since the WS upgrade handshake can't reliably
// carry custom headers from all client types.
func (s *Server) handleDriverWS(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	deviceID := r.URL.Query().Get("device_id")
	if token == "" || deviceID == "" {
		http.Error(w, "token and device_id are required", http.StatusUnauthorized)
		return
	}

	claims, err := s.verifier.Parse(token)
	if err != nil || claims.Role != auth.DriverRole {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	driverID, err := uuid.Parse(claims.UserID)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("driver_realtime_gateway: ws upgrade failed driver_id=%s: %v", driverID, err)
		return
	}

	wsConn := NewConnection(context.Background(), driverID, deviceID, conn, s.pingInterval, s.pongWait, s.onAck)
	s.hub.Register(wsConn)
	log.Printf("driver_realtime_gateway: driver connected driver_id=%s device_id=%s", driverID, deviceID)

	if s.onConnect != nil {
		s.onConnect(r.Context(), wsConn)
	}

	go func() {
		<-wsConn.Done()
		s.hub.Unregister(wsConn)
		log.Printf("driver_realtime_gateway: driver disconnected driver_id=%s device_id=%s", driverID, deviceID)
	}()
}

// handleRiderWS upgrades GET /ws/rider?token=<jwt>&device_id=<id>. Same
// query-param token pattern as the driver route, but requires RiderRole.
func (s *Server) handleRiderWS(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	deviceID := r.URL.Query().Get("device_id")
	if token == "" || deviceID == "" {
		http.Error(w, "token and device_id are required", http.StatusUnauthorized)
		return
	}

	claims, err := s.verifier.Parse(token)
	if err != nil || claims.Role != auth.RiderRole {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	riderID, err := uuid.Parse(claims.UserID)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("driver_realtime_gateway: rider ws upgrade failed rider_id=%s: %v", riderID, err)
		return
	}

	riderConn := NewRiderConnection(context.Background(), riderID, deviceID, conn, s.pingInterval, s.pongWait)
	s.riderHub.Register(riderConn)
	log.Printf("driver_realtime_gateway: rider connected rider_id=%s device_id=%s", riderID, deviceID)

	go func() {
		<-riderConn.Done()
		s.riderHub.Unregister(riderConn)
		log.Printf("driver_realtime_gateway: rider disconnected rider_id=%s device_id=%s", riderID, deviceID)
	}()
}
