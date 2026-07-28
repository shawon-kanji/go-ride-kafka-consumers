package bootstrap

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"go-ride-kafka-consumers/services/driver-request-handler/internal/api"
	"go-ride-kafka-consumers/services/driver-request-handler/internal/auth"
	"go-ride-kafka-consumers/services/driver-request-handler/internal/config"
	"go-ride-kafka-consumers/services/driver-request-handler/internal/db"
	"go-ride-kafka-consumers/services/driver-request-handler/internal/kafka"
	"go-ride-kafka-consumers/services/driver-request-handler/internal/offers"
)

type App struct {
	cfg      config.Config
	sqlDB    *sql.DB
	server   *api.Server
	producer kafka.Producer
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

	producer := kafka.NewRideAssignedProducer(cfg)
	verifier := auth.NewVerifier(cfg.JWTSecret, cfg.JWTIssuer, cfg.JWTAudience)
	offerService := offers.NewService(gormDB, producer)
	server := api.NewServer(cfg, verifier, offerService, producer)

	return &App{
		cfg:      cfg,
		sqlDB:    sqlDB,
		server:   server,
		producer: producer,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	log.Printf("starting service=%s brokers=%v addr=%s", a.cfg.ServiceName, a.cfg.KafkaBrokers, a.cfg.HTTPAddr)

	defer func() {
		if err := a.sqlDB.Close(); err != nil {
			log.Printf("close sql db: %v", err)
		}
	}()

	if err := a.sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sql db: %w", err)
	}

	if err := a.server.Start(ctx); err != nil {
		return fmt.Errorf("start server: %w", err)
	}

	log.Printf("service stopped=%s", a.cfg.ServiceName)
	return nil
}
