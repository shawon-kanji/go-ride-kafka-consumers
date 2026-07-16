package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	ServiceName   string
	LogLevel      string
	DB            DBConfig
	KafkaBrokers  []string
	KafkaTopic    string
	ConsumerGroup string
}

type DBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Name     string
	SSLMode  string
}

func Load() (Config, error) {
	dbPort, err := getIntEnv("DB_PORT", 5432)
	if err != nil {
		return Config{}, fmt.Errorf("invalid DB_PORT: %w", err)
	}

	cfg := Config{
		ServiceName: getEnv("SERVICE_NAME", "location-worker"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
		DB: DBConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     dbPort,
			User:     getEnv("DB_USER", "postgres"),
			Password: getEnv("DB_PASSWORD", "postgres"),
			Name:     getEnv("DB_NAME", "go_ride"),
			SSLMode:  getEnv("DB_SSLMODE", "disable"),
		},
		KafkaTopic:    getEnv("KAFKA_TOPIC", "driver.location.updated.v1"),
		ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "location-worker-group"),
		KafkaBrokers:  splitAndTrim(getEnv("KAFKA_BROKERS", "localhost:9094")),
	}

	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS is required")
	}
	if cfg.KafkaTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_TOPIC is required")
	}
	if cfg.ConsumerGroup == "" {
		return Config{}, fmt.Errorf("KAFKA_CONSUMER_GROUP is required")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return fallback
	}
	return val
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

func getIntEnv(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	return strconv.Atoi(value)
}
