package bootstrap

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/assignments"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/auth"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/config"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/db"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/kafka"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/offers"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/presence"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/tracking"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/tripcancel"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/tripcomplete"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/tripend"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/tripstart"
	"go-ride-kafka-consumers/services/websocket-gateway/internal/ws"

	"github.com/google/uuid"
)

type App struct {
	cfg                   config.Config
	sqlDB                 *sql.DB
	bus                   *presence.Bus
	assignmentBus         *presence.Bus
	locationBus           *presence.Bus
	tripStartedBus        *presence.Bus
	tripEndedBus          *presence.Bus
	tripCompletedBus      *presence.Bus
	tripCancelledBus      *presence.Bus
	locationStore         *presence.Store
	consumer              kafka.Consumer
	assignmentConsumer    kafka.Consumer
	locationConsumer      kafka.Consumer
	rideStartedConsumer   kafka.Consumer
	rideEndedConsumer     kafka.Consumer
	rideCompletedConsumer kafka.Consumer
	rideCancelledConsumer kafka.Consumer
	server                *http.Server
}

func New(ctx context.Context) (*App, error) {
	cfg, err := config.Load(ctx)
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
	replayer := offers.NewReplayer(gormDB, store, cfg.ReplayBatch, cfg.DriverCommissionRate, cfg.FallbackAvgSpeedKPH)

	hub := ws.NewHub()
	deliverer := offers.NewDeliverer(hub, store)

	bus := presence.NewBus(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.RedisChannel)
	notifier := offers.NewNotifier(bus)
	consumer := kafka.NewOfferConsumer(cfg, notifier)

	riderHub := ws.NewRiderHub()
	assignmentDeliverer := assignments.NewDeliverer(riderHub)

	locationStore := presence.NewStore(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.ActiveTripTTL)

	assignmentBus := presence.NewBus(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.RedisAssignmentChannel)
	assignmentNotifier := assignments.NewNotifier(assignmentBus, locationStore)
	assignmentConsumer := kafka.NewAssignmentConsumer(cfg, assignmentNotifier)

	locationDeliverer := tracking.NewDeliverer(riderHub)
	locationBus := presence.NewBus(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.RedisLocationChannel)
	locationNotifier := tracking.NewNotifier(locationStore, locationBus, cfg.FallbackAvgSpeedKPH)
	locationConsumer := kafka.NewLocationConsumer(cfg, locationNotifier)

	tripStartedDeliverer := tripstart.NewDeliverer(riderHub)
	tripStartedBus := presence.NewBus(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.RedisTripStartedChannel)
	tripStartedNotifier := tripstart.NewNotifier(locationStore, tripStartedBus)
	rideStartedConsumer := kafka.NewRideStartedConsumer(cfg, tripStartedNotifier)

	tripEndedDeliverer := tripend.NewDeliverer(riderHub)
	tripEndedBus := presence.NewBus(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.RedisTripEndedChannel)
	tripEndedNotifier := tripend.NewNotifier(tripEndedBus)
	rideEndedConsumer := kafka.NewRideEndedConsumer(cfg, tripEndedNotifier)

	tripCompletedDeliverer := tripcomplete.NewDeliverer(riderHub)
	tripCompletedBus := presence.NewBus(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.RedisTripCompletedChannel)
	tripCompletedNotifier := tripcomplete.NewNotifier(tripCompletedBus)
	rideCompletedConsumer := kafka.NewRideCompletedConsumer(cfg, tripCompletedNotifier)

	tripCancelledDeliverer := tripcancel.NewDeliverer(riderHub, hub)
	tripCancelledBus := presence.NewBus(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.RedisTripCancelledChannel)
	tripCancelledNotifier := tripcancel.NewNotifier(locationStore, tripCancelledBus)
	rideCancelledConsumer := kafka.NewRideCancelledConsumer(cfg, tripCancelledNotifier)

	verifier := auth.NewVerifier(cfg.JWTSecret, cfg.JWTIssuer)

	onConnect := func(ctx context.Context, conn *ws.Connection) {
		if err := replayer.Replay(ctx, conn); err != nil {
			log.Printf("websocket_gateway: replay failed driver_id=%s device_id=%s: %v", conn.DriverID, conn.DeviceID, err)
		}
	}
	onAck := func(ctx context.Context, ack ws.AckMessage) {
		jobOfferID, err := uuid.Parse(ack.JobOfferID)
		if err != nil {
			log.Printf("websocket_gateway: invalid ack job_offer_id=%q: %v", ack.JobOfferID, err)
			return
		}
		if err := store.MarkAck(ctx, jobOfferID, ack.Status); err != nil {
			log.Printf("websocket_gateway: mark ack failed job_offer_id=%s status=%s: %v", jobOfferID, ack.Status, err)
		}
	}

	wsServer := ws.NewServer(hub, riderHub, verifier, cfg.JWTDriverAudience, cfg.JWTRiderAudience, cfg.PingInterval, cfg.PongWait, onConnect, onAck)
	mux := http.NewServeMux()
	wsServer.Routes(mux)

	httpServer := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: mux,
	}

	go func() {
		bgCtx := context.Background()
		if err := bus.Subscribe(bgCtx, func(payload []byte) {
			deliverer.HandleBroadcast(bgCtx, payload)
		}); err != nil {
			log.Printf("websocket_gateway: presence subscribe stopped: %v", err)
		}
	}()

	go func() {
		bgCtx := context.Background()
		if err := assignmentBus.Subscribe(bgCtx, func(payload []byte) {
			assignmentDeliverer.HandleBroadcast(bgCtx, payload)
		}); err != nil {
			log.Printf("websocket_gateway: assignment presence subscribe stopped: %v", err)
		}
	}()

	go func() {
		bgCtx := context.Background()
		if err := locationBus.Subscribe(bgCtx, func(payload []byte) {
			locationDeliverer.HandleBroadcast(bgCtx, payload)
		}); err != nil {
			log.Printf("websocket_gateway: location presence subscribe stopped: %v", err)
		}
	}()

	go func() {
		bgCtx := context.Background()
		if err := tripStartedBus.Subscribe(bgCtx, func(payload []byte) {
			tripStartedDeliverer.HandleBroadcast(bgCtx, payload)
		}); err != nil {
			log.Printf("websocket_gateway: trip started presence subscribe stopped: %v", err)
		}
	}()

	go func() {
		bgCtx := context.Background()
		if err := tripEndedBus.Subscribe(bgCtx, func(payload []byte) {
			tripEndedDeliverer.HandleBroadcast(bgCtx, payload)
		}); err != nil {
			log.Printf("websocket_gateway: trip ended presence subscribe stopped: %v", err)
		}
	}()

	go func() {
		bgCtx := context.Background()
		if err := tripCompletedBus.Subscribe(bgCtx, func(payload []byte) {
			tripCompletedDeliverer.HandleBroadcast(bgCtx, payload)
		}); err != nil {
			log.Printf("websocket_gateway: trip completed presence subscribe stopped: %v", err)
		}
	}()

	go func() {
		bgCtx := context.Background()
		if err := tripCancelledBus.Subscribe(bgCtx, func(payload []byte) {
			tripCancelledDeliverer.HandleBroadcast(bgCtx, payload)
		}); err != nil {
			log.Printf("websocket_gateway: trip cancelled presence subscribe stopped: %v", err)
		}
	}()

	return &App{
		cfg:                   cfg,
		sqlDB:                 sqlDB,
		bus:                   bus,
		assignmentBus:         assignmentBus,
		locationBus:           locationBus,
		tripStartedBus:        tripStartedBus,
		tripEndedBus:          tripEndedBus,
		tripCompletedBus:      tripCompletedBus,
		tripCancelledBus:      tripCancelledBus,
		locationStore:         locationStore,
		consumer:              consumer,
		assignmentConsumer:    assignmentConsumer,
		locationConsumer:      locationConsumer,
		rideStartedConsumer:   rideStartedConsumer,
		rideEndedConsumer:     rideEndedConsumer,
		rideCompletedConsumer: rideCompletedConsumer,
		rideCancelledConsumer: rideCancelledConsumer,
		server:                httpServer,
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
		if err := a.assignmentBus.Close(); err != nil {
			log.Printf("close assignment redis bus: %v", err)
		}
		if err := a.locationBus.Close(); err != nil {
			log.Printf("close location redis bus: %v", err)
		}
		if err := a.tripStartedBus.Close(); err != nil {
			log.Printf("close trip started redis bus: %v", err)
		}
		if err := a.tripEndedBus.Close(); err != nil {
			log.Printf("close trip ended redis bus: %v", err)
		}
		if err := a.tripCompletedBus.Close(); err != nil {
			log.Printf("close trip completed redis bus: %v", err)
		}
		if err := a.tripCancelledBus.Close(); err != nil {
			log.Printf("close trip cancelled redis bus: %v", err)
		}
		if err := a.locationStore.Close(); err != nil {
			log.Printf("close location redis store: %v", err)
		}
	}()

	if err := a.sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sql db: %w", err)
	}

	errCh := make(chan error, 8)

	go func() {
		if err := a.consumer.Start(ctx); err != nil {
			errCh <- fmt.Errorf("kafka consumer: %w", err)
			return
		}
		errCh <- nil
	}()

	go func() {
		if err := a.assignmentConsumer.Start(ctx); err != nil {
			errCh <- fmt.Errorf("assignment kafka consumer: %w", err)
			return
		}
		errCh <- nil
	}()

	go func() {
		if err := a.locationConsumer.Start(ctx); err != nil {
			errCh <- fmt.Errorf("location kafka consumer: %w", err)
			return
		}
		errCh <- nil
	}()

	go func() {
		if err := a.rideStartedConsumer.Start(ctx); err != nil {
			errCh <- fmt.Errorf("ride started kafka consumer: %w", err)
			return
		}
		errCh <- nil
	}()

	go func() {
		if err := a.rideEndedConsumer.Start(ctx); err != nil {
			errCh <- fmt.Errorf("ride ended kafka consumer: %w", err)
			return
		}
		errCh <- nil
	}()

	go func() {
		if err := a.rideCompletedConsumer.Start(ctx); err != nil {
			errCh <- fmt.Errorf("ride completed kafka consumer: %w", err)
			return
		}
		errCh <- nil
	}()

	go func() {
		if err := a.rideCancelledConsumer.Start(ctx); err != nil {
			errCh <- fmt.Errorf("ride cancelled kafka consumer: %w", err)
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
	for i := 0; i < 8; i++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
		}
	}

	log.Printf("service stopped=%s", a.cfg.ServiceName)
	return firstErr
}
