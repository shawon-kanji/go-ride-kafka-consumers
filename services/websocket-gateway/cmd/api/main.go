package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"go-ride-kafka-consumers/services/websocket-gateway/internal/bootstrap"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := bootstrap.New(ctx)
	if err != nil {
		log.Fatalf("bootstrap app: %v", err)
	}

	if err := app.Run(ctx); err != nil {
		log.Fatalf("run api: %v", err)
	}
}
