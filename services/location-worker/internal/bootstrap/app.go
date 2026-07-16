package bootstrap

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"go-ride-kafka-consumers/services/location-worker/internal/config"
	"go-ride-kafka-consumers/services/location-worker/internal/db"
	"go-ride-kafka-consumers/services/location-worker/internal/kafka"
	"go-ride-kafka-consumers/services/location-worker/internal/worker"
)

type App struct {
	cfg      config.Config
	sqlDB    *sql.DB
	consumer kafka.Consumer
	runner   *worker.Runner
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

	consumer := kafka.NewNoopConsumer()
	runner := worker.NewRunner(consumer)

	return &App{
		cfg:      cfg,
		sqlDB:    sqlDB,
		consumer: consumer,
		runner:   runner,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	log.Printf("starting service=%s topic=%s group=%s brokers=%v", a.cfg.ServiceName, a.cfg.KafkaTopic, a.cfg.ConsumerGroup, a.cfg.KafkaBrokers)

	defer func() {
		if err := a.sqlDB.Close(); err != nil {
			log.Printf("close sql db: %v", err)
		}
	}()

	if err := a.sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sql db: %w", err)
	}

	if err := a.runner.Run(ctx); err != nil {
		return fmt.Errorf("worker runner: %w", err)
	}

	log.Printf("service stopped=%s", a.cfg.ServiceName)
	return nil
}
