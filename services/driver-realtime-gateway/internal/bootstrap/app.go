package bootstrap

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"

	"go-ride-kafka-consumers/services/driver-realtime-gateway/internal/auth"
	"go-ride-kafka-consumers/services/driver-realtime-gateway/internal/config"
	"go-ride-kafka-consumers/services/driver-realtime-gateway/internal/db"
	"go-ride-kafka-consumers/services/driver-realtime-gateway/internal/kafka"
	"go-ride-kafka-consumers/services/driver-realtime-gateway/internal/offers"
	"go-ride-kafka-consumers/services/driver-realtime-gateway/internal/presence"
	"go-ride-kafka-consumers/services/driver-realtime-gateway/internal/ws"

	"github.com/google/uuid"
)

type App struct {
	cfg      config.Config
	sqlDB    *sql.DB
	bus      *presence.Bus
	consumer kafka.Consumer
	server   *http.Server
}

func New() (*App, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	gormDB, err := db.NewGorm(cfg.DB)
	if err != nil {
		return nil, fmt.Errorf("init gorm db: %w", err)
	}

	sqlDB, err := gormDB.DB()
	if err != nil {
		return nil, fmt.Errorf("extract sql db: %w", err)
	}

	store := offers.NewStore(gormDB)
	replayer := offers.NewReplayer(gormDB, store, cfg.ReplayBatch, cfg.DriverCommissionRate)

	hub := ws.NewHub()
	deliverer := offers.NewDeliverer(hub, store)

	bus := presence.NewBus(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.RedisChannel)
	notifier := offers.NewNotifier(bus)
	consumer := kafka.NewOfferConsumer(cfg, notifier)

	verifier := auth.NewVerifier(cfg.JWTSecret, cfg.JWTIssuer, cfg.JWTAudience)

	onConnect := func(ctx context.Context, conn *ws.Connection) {
		if err := replayer.Replay(ctx, conn); err != nil {
			log.Printf("driver_realtime_gateway: replay failed driver_id=%s device_id=%s: %v", conn.DriverID, conn.DeviceID, err)
		}
	}
	onAck := func(ctx context.Context, ack ws.AckMessage) {
		jobOfferID, err := uuid.Parse(ack.JobOfferID)
		if err != nil {
			log.Printf("driver_realtime_gateway: invalid ack job_offer_id=%q: %v", ack.JobOfferID, err)
			return
		}
		if err := store.MarkAck(ctx, jobOfferID, ack.Status); err != nil {
			log.Printf("driver_realtime_gateway: mark ack failed job_offer_id=%s status=%s: %v", jobOfferID, ack.Status, err)
		}
	}

	wsServer := ws.NewServer(hub, verifier, cfg.PingInterval, cfg.PongWait, onConnect, onAck)
	mux := http.NewServeMux()
	wsServer.Routes(mux)

	httpServer := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: mux,
	}

	go func() {
		bgCtx := context.Background()
		if err := bus.Subscribe(bgCtx, func(n presence.Notification) {
			deliverer.HandleNotification(bgCtx, n)
		}); err != nil {
			log.Printf("driver_realtime_gateway: presence subscribe stopped: %v", err)
		}
	}()

	return &App{
		cfg:      cfg,
		sqlDB:    sqlDB,
		bus:      bus,
		consumer: consumer,
		server:   httpServer,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	log.Printf("starting service=%s addr=%s topic=%s brokers=%v", a.cfg.ServiceName, a.cfg.HTTPAddr, a.cfg.OfferCreatedTopic, a.cfg.KafkaBrokers)

	defer func() {
		if err := a.sqlDB.Close(); err != nil {
			log.Printf("close sql db: %v", err)
		}
		if err := a.bus.Close(); err != nil {
			log.Printf("close redis bus: %v", err)
		}
	}()

	if err := a.sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sql db: %w", err)
	}

	errCh := make(chan error, 2)

	go func() {
		if err := a.consumer.Start(ctx); err != nil {
			errCh <- fmt.Errorf("kafka consumer: %w", err)
			return
		}
		errCh <- nil
	}()

	go func() {
		if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("http server: %w", err)
			return
		}
		errCh <- nil
	}()

	go func() {
		<-ctx.Done()
		_ = a.server.Shutdown(context.Background())
	}()

	var firstErr error
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
		}
	}

	log.Printf("service stopped=%s", a.cfg.ServiceName)
	return firstErr
}
