package worker

import (
	"context"
	"fmt"

	"go-ride-kafka-consumers/services/location-consumers/internal/kafka"
)

type Runner struct {
	consumer kafka.Consumer
}

func NewRunner(consumer kafka.Consumer) *Runner {
	return &Runner{consumer: consumer}
}

func (r *Runner) Run(ctx context.Context) error {
	if r.consumer == nil {
		return fmt.Errorf("consumer is nil")
	}
	return r.consumer.Start(ctx)
}
