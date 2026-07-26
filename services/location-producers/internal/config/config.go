package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/shawon-kanji/go-ride-utils/kafkatopics"
)

type Config struct {
	ServiceName  string
	HTTPAddr     string
	KafkaBrokers []string
	KafkaTopic   string
}

func Load() (Config, error) {
	cfg := Config{
		ServiceName:  getEnv("SERVICE_NAME", "location-producers"),
		HTTPAddr:     getEnv("HTTP_ADDR", ":8081"),
		KafkaBrokers: splitAndTrim(getEnv("KAFKA_BROKERS", "localhost:9094")),
		KafkaTopic:   getEnv("KAFKA_TOPIC", kafkatopics.DriverLocationUpdatedV1),
	}

	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS is required")
	}
	if cfg.KafkaTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_TOPIC is required")
	}
	if cfg.HTTPAddr == "" {
		return Config{}, fmt.Errorf("HTTP_ADDR is required")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func splitAndTrim(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}
