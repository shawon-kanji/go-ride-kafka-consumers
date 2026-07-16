package config

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	ServiceName   string
	Mode          string
	KafkaBrokers  []string
	ConsumerGroup string
	InputTopic    string
	AssignedTopic string
	UnassignTopic string
}

func Load(mode string) (Config, error) {
	cfg := Config{
		ServiceName:   getEnv("SERVICE_NAME", "trip-dispatch-worker"),
		Mode:          mode,
		KafkaBrokers:  splitAndTrim(getEnv("KAFKA_BROKERS", "localhost:9094")),
		ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "trip-dispatch-worker-group"),
		InputTopic:    getEnv("KAFKA_INPUT_TOPIC", "ride.requested.v1"),
		AssignedTopic: getEnv("KAFKA_ASSIGNED_TOPIC", "ride.assigned.v1"),
		UnassignTopic: getEnv("KAFKA_UNASSIGNED_TOPIC", "ride.unassigned.v1"),
	}

	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS is required")
	}
	if cfg.InputTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_INPUT_TOPIC is required")
	}
	if cfg.AssignedTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_ASSIGNED_TOPIC is required")
	}
	if cfg.UnassignTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_UNASSIGNED_TOPIC is required")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func splitAndTrim(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		v := strings.TrimSpace(part)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}
