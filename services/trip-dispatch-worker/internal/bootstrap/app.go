package bootstrap

import (
	"context"
	"fmt"
	"log"

	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/config"
	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/kafka"
	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/worker"
)

type App struct {
	cfg      config.Config
	runner   *worker.Runner
	consumer kafka.Consumer
}

func New(mode string) (*App, error) {
	cfg, err := config.Load(mode)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	consumer := kafka.NewNoopConsumer(cfg.ServiceName, cfg.InputTopic)
	runner := worker.NewRunner(consumer)

	return &App{
		cfg:      cfg,
		runner:   runner,
		consumer: consumer,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	log.Printf("starting service=%s mode=%s topic=%s group=%s brokers=%v", a.cfg.ServiceName, a.cfg.Mode, a.cfg.InputTopic, a.cfg.ConsumerGroup, a.cfg.KafkaBrokers)

	if a.cfg.Mode == "api" {
		<-ctx.Done()
		log.Printf("api mode stopped service=%s", a.cfg.ServiceName)
		return nil
	}

	if err := a.runner.Run(ctx); err != nil {
		return fmt.Errorf("worker runner: %w", err)
	}

	log.Printf("service stopped=%s", a.cfg.ServiceName)
	return nil
}
