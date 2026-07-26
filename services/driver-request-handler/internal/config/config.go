package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	ServiceName   string
	HTTPAddr      string
	DB            DBConfig
	KafkaBrokers  []string
	AssignedTopic string
	StartedTopic  string
	JWTSecret     string
	JWTIssuer     string
	JWTAudience   string
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
		ServiceName: getEnv("SERVICE_NAME", "driver-request-handler"),
		HTTPAddr:    getEnv("HTTP_ADDR", ":8084"),
		DB: DBConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     dbPort,
			User:     getEnv("DB_USER", "postgres"),
			Password: getEnv("DB_PASSWORD", "postgres"),
			Name:     getEnv("DB_NAME", "go_ride"),
			SSLMode:  getEnv("DB_SSLMODE", "disable"),
		},
		KafkaBrokers:  splitAndTrim(getEnv("KAFKA_BROKERS", "localhost:9094")),
		AssignedTopic: getEnv("KAFKA_ASSIGNED_TOPIC", "ride.assigned.v1"),
		StartedTopic:  getEnv("KAFKA_STARTED_TOPIC", "ride.started.v1"),
		JWTSecret:     getEnv("JWT_SECRET", ""),
		JWTIssuer:     getEnv("JWT_ISSUER", "go-ride-backend"),
		JWTAudience:   getEnv("JWT_AUDIENCE", "go-ride-drivers"),
	}

	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS is required")
	}
	if cfg.AssignedTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_ASSIGNED_TOPIC is required")
	}
	if cfg.StartedTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_STARTED_TOPIC is required")
	}
	if cfg.HTTPAddr == "" {
		return Config{}, fmt.Errorf("HTTP_ADDR is required")
	}
	if cfg.JWTSecret == "" {
		return Config{}, fmt.Errorf("JWT_SECRET is required")
	}
	if cfg.JWTIssuer == "" {
		return Config{}, fmt.Errorf("JWT_ISSUER is required")
	}
	if cfg.JWTAudience == "" {
		return Config{}, fmt.Errorf("JWT_AUDIENCE is required")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
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

func getIntEnv(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	return strconv.Atoi(value)
}
