package worker

import (
	"context"
	"fmt"

	"go-ride-kafka-consumers/services/trip-dispatch-worker/internal/kafka"
)

type Runner struct {
	consumer kafka.Consumer
}

func NewRunner(consumer kafka.Consumer) *Runner {
	return &Runner{consumer: consumer}
}

func (r *Runner) Run(ctx context.Context) error {
	if err := r.consumer.Start(ctx); err != nil {
		return fmt.Errorf("start consumer: %w", err)
	}
	return nil
}
