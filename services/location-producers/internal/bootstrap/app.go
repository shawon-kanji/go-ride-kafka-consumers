package bootstrap

import (
	"context"
	"fmt"

	locationapi "go-ride-kafka-consumers/services/location-producers/internal/api"
	"go-ride-kafka-consumers/services/location-producers/internal/config"
	"go-ride-kafka-consumers/services/location-producers/internal/kafka"
)

type App struct {
	server *locationapi.Server
}

func New() (*App, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	producer := kafka.NewDriverLocationProducer(cfg)
	server := locationapi.NewServer(cfg, producer)

	return &App{server: server}, nil
}

func (a *App) Run(ctx context.Context) error {
	return a.server.Start(ctx)
}
