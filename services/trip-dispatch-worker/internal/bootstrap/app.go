package bootstrap

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/config"
	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/db"
	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/dispatch"
	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/kafka"
	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/worker"
)

type App struct {
	cfg            config.Config
	sqlDB          *sql.DB
	consumer       kafka.Consumer
	cancelConsumer kafka.Consumer
	producer       kafka.Producer
	runner         *worker.Runner
	sweepRunner    *worker.SweepRunner
}

func New(ctx context.Context, mode string) (*App, error) {
	cfg, err := config.Load(ctx, mode)
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

	producer := kafka.NewTripDispatcherProducer(cfg)
	service := dispatch.NewService(gormDB, cfg, producer)
	consumer := kafka.NewDispatchConsumer(cfg, service)
	cancelConsumer := kafka.NewRideCancelledConsumer(cfg, service)
	runner := worker.NewRunner(consumer)
	sweepRunner := worker.NewSweepRunner(gormDB, service, cfg.DispatchSweepInterval, cfg.DispatchMaxAttempts)

	return &App{
		cfg:            cfg,
		sqlDB:          sqlDB,
		consumer:       consumer,
		cancelConsumer: cancelConsumer,
		producer:       producer,
		runner:         runner,
		sweepRunner:    sweepRunner,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	log.Printf("starting service=%s mode=%s topic=%s group=%s brokers=%v", a.cfg.ServiceName, a.cfg.Mode, a.cfg.InputTopic, a.cfg.ConsumerGroup, a.cfg.KafkaBrokers)

	defer func() {
		if err := a.sqlDB.Close(); err != nil {
			log.Printf("close sql db: %v", err)
		}
		if err := a.producer.Close(); err != nil {
			log.Printf("close kafka producer: %v", err)
		}
	}()

	if err := a.sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sql db: %w", err)
	}

	if a.cfg.Mode == "api" {
		<-ctx.Done()
		log.Printf("api mode stopped service=%s", a.cfg.ServiceName)
		return nil
	}

	sweepCtx, cancelSweep := context.WithCancel(ctx)
	defer cancelSweep()
	go func() { _ = a.sweepRunner.Run(sweepCtx) }()

	// The dispatch consumer and the ride-cancelled/redispatch consumer run as
	// two independent goroutines fanning into one error channel. A real
	// failure in either cancels runCtx so the other stops too (it will see
	// context.Canceled and return nil, a clean stop — the first real error is
	// what gets returned), while a normal shutdown via the parent ctx lets
	// both stop cleanly on their own.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	consumerErrCh := make(chan error, 2)
	go func() {
		err := a.runner.Run(runCtx)
		if err != nil {
			cancelRun()
		}
		consumerErrCh <- err
	}()
	go func() {
		err := a.cancelConsumer.Start(runCtx)
		if err != nil {
			cancelRun()
		}
		consumerErrCh <- err
	}()

	var firstErr error
	for i := 0; i < 2; i++ {
		if err := <-consumerErrCh; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return fmt.Errorf("worker runner: %w", firstErr)
	}

	log.Printf("service stopped=%s", a.cfg.ServiceName)
	return nil
}
