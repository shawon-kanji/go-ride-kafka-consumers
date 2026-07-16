package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"go-ride-kafka-consumers/services/location-producers/internal/bootstrap"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := bootstrap.New()
	if err != nil {
		log.Fatalf("bootstrap app: %v", err)
	}

	if err := app.Run(ctx); err != nil {
		log.Fatalf("run ingest api: %v", err)
	}
}
