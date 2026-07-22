package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName       string
	Mode              string
	DB                DBConfig
	KafkaBrokers      []string
	ConsumerGroup     string
	InputTopic        string
	AssignedTopic     string
	UnassignTopic     string
	OfferCreatedTopic string

	NearestDriversLimit       int
	DriverLocationFreshWindow time.Duration
	DispatchInitialRadiusKM   float64
	DispatchRadiusStepKM      float64
	DispatchMaxRadiusKM       float64
	DispatchMaxAttempts       int
	DispatchBackoffBaseSecs   int
	DispatchBackoffMaxSecs    int
	JobOfferTTLSeconds        int
	DispatchSweepInterval     time.Duration
}

type DBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Name     string
	SSLMode  string
}

func Load(mode string) (Config, error) {
	dbPort, err := getIntEnv("DB_PORT", 5432)
	if err != nil {
		return Config{}, fmt.Errorf("invalid DB_PORT: %w", err)
	}

	nearestDriversLimit, err := getIntEnv("NEAREST_DRIVERS_LIMIT", 10)
	if err != nil {
		return Config{}, fmt.Errorf("invalid NEAREST_DRIVERS_LIMIT: %w", err)
	}

	driverLocationFreshWindowSeconds, err := getIntEnv("DRIVER_LOCATION_FRESH_WINDOW_SECONDS", 300)
	if err != nil {
		return Config{}, fmt.Errorf("invalid DRIVER_LOCATION_FRESH_WINDOW_SECONDS: %w", err)
	}

	dispatchInitialRadiusKM, err := getFloatEnv("DISPATCH_INITIAL_RADIUS_KM", 20)
	if err != nil {
		return Config{}, fmt.Errorf("invalid DISPATCH_INITIAL_RADIUS_KM: %w", err)
	}

	dispatchRadiusStepKM, err := getFloatEnv("DISPATCH_RADIUS_STEP_KM", 5)
	if err != nil {
		return Config{}, fmt.Errorf("invalid DISPATCH_RADIUS_STEP_KM: %w", err)
	}

	dispatchMaxRadiusKM, err := getFloatEnv("DISPATCH_MAX_RADIUS_KM", 30)
	if err != nil {
		return Config{}, fmt.Errorf("invalid DISPATCH_MAX_RADIUS_KM: %w", err)
	}

	dispatchMaxAttempts, err := getIntEnv("DISPATCH_MAX_ATTEMPTS", 5)
	if err != nil {
		return Config{}, fmt.Errorf("invalid DISPATCH_MAX_ATTEMPTS: %w", err)
	}

	dispatchBackoffBaseSecs, err := getIntEnv("DISPATCH_BACKOFF_BASE_SECONDS", 2)
	if err != nil {
		return Config{}, fmt.Errorf("invalid DISPATCH_BACKOFF_BASE_SECONDS: %w", err)
	}

	dispatchBackoffMaxSecs, err := getIntEnv("DISPATCH_BACKOFF_MAX_SECONDS", 30)
	if err != nil {
		return Config{}, fmt.Errorf("invalid DISPATCH_BACKOFF_MAX_SECONDS: %w", err)
	}

	jobOfferTTLSeconds, err := getIntEnv("JOB_OFFER_TTL_SECONDS", 15)
	if err != nil {
		return Config{}, fmt.Errorf("invalid JOB_OFFER_TTL_SECONDS: %w", err)
	}

	dispatchSweepIntervalSeconds, err := getIntEnv("DISPATCH_SWEEP_INTERVAL_SECONDS", 3)
	if err != nil {
		return Config{}, fmt.Errorf("invalid DISPATCH_SWEEP_INTERVAL_SECONDS: %w", err)
	}

	cfg := Config{
		ServiceName: getEnv("SERVICE_NAME", "trip-dispatch-worker"),
		Mode:        mode,
		DB: DBConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     dbPort,
			User:     getEnv("DB_USER", "postgres"),
			Password: getEnv("DB_PASSWORD", "postgres"),
			Name:     getEnv("DB_NAME", "go_ride"),
			SSLMode:  getEnv("DB_SSLMODE", "disable"),
		},
		KafkaBrokers:      splitAndTrim(getEnv("KAFKA_BROKERS", "localhost:9094")),
		ConsumerGroup:     getEnv("KAFKA_CONSUMER_GROUP", "trip-dispatch-worker-group"),
		InputTopic:        getEnv("KAFKA_INPUT_TOPIC", "ride.requested.v1"),
		AssignedTopic:     getEnv("KAFKA_ASSIGNED_TOPIC", "ride.assigned.v1"),
		UnassignTopic:     getEnv("KAFKA_UNASSIGNED_TOPIC", "ride.unassigned.v1"),
		OfferCreatedTopic: getEnv("KAFKA_OFFER_CREATED_TOPIC", "driver.job_offer.created.v1"),

		NearestDriversLimit:       nearestDriversLimit,
		DriverLocationFreshWindow: time.Duration(driverLocationFreshWindowSeconds) * time.Second,
		DispatchInitialRadiusKM:   dispatchInitialRadiusKM,
		DispatchRadiusStepKM:      dispatchRadiusStepKM,
		DispatchMaxRadiusKM:       dispatchMaxRadiusKM,
		DispatchMaxAttempts:       dispatchMaxAttempts,
		DispatchBackoffBaseSecs:   dispatchBackoffBaseSecs,
		DispatchBackoffMaxSecs:    dispatchBackoffMaxSecs,
		JobOfferTTLSeconds:        jobOfferTTLSeconds,
		DispatchSweepInterval:     time.Duration(dispatchSweepIntervalSeconds) * time.Second,
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
	if cfg.OfferCreatedTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_OFFER_CREATED_TOPIC is required")
	}
	if cfg.NearestDriversLimit <= 0 {
		return Config{}, fmt.Errorf("NEAREST_DRIVERS_LIMIT must be greater than zero")
	}
	if cfg.DispatchMaxAttempts <= 0 {
		return Config{}, fmt.Errorf("DISPATCH_MAX_ATTEMPTS must be greater than zero")
	}
	if cfg.JobOfferTTLSeconds <= 0 {
		return Config{}, fmt.Errorf("JOB_OFFER_TTL_SECONDS must be greater than zero")
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

func getIntEnv(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	return strconv.Atoi(value)
}

func getFloatEnv(key string, fallback float64) (float64, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	return strconv.ParseFloat(value, 64)
}
