package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName           string
	HTTPAddr              string
	DB                    DBConfig
	KafkaBrokers          []string
	KafkaTopic            string
	DefaultSearchRadiusKM float64
	FareCurrencyCode      string
	FarePricingVersion    string
	FareBaseAmount        float64
	FarePerKmAmount       float64
	FarePerMinuteAmount   float64
	FareMinimumAmount     float64
	FareAverageSpeedKPH   float64
	FareLockTTL           time.Duration
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

	fareLockTTLMinutes, err := getIntEnv("FARE_LOCK_TTL_MINUTES", 15)
	if err != nil {
		return Config{}, fmt.Errorf("invalid FARE_LOCK_TTL_MINUTES: %w", err)
	}

	defaultSearchRadiusKM, err := getFloatEnv("DEFAULT_SEARCH_RADIUS_KM", 20)
	if err != nil {
		return Config{}, fmt.Errorf("invalid DEFAULT_SEARCH_RADIUS_KM: %w", err)
	}

	fareBaseAmount, err := getFloatEnv("FARE_BASE_AMOUNT", 5)
	if err != nil {
		return Config{}, fmt.Errorf("invalid FARE_BASE_AMOUNT: %w", err)
	}

	farePerKmAmount, err := getFloatEnv("FARE_PER_KM_AMOUNT", 1.5)
	if err != nil {
		return Config{}, fmt.Errorf("invalid FARE_PER_KM_AMOUNT: %w", err)
	}

	farePerMinuteAmount, err := getFloatEnv("FARE_PER_MINUTE_AMOUNT", 0.25)
	if err != nil {
		return Config{}, fmt.Errorf("invalid FARE_PER_MINUTE_AMOUNT: %w", err)
	}

	fareMinimumAmount, err := getFloatEnv("FARE_MINIMUM_AMOUNT", 8)
	if err != nil {
		return Config{}, fmt.Errorf("invalid FARE_MINIMUM_AMOUNT: %w", err)
	}

	fareAverageSpeedKPH, err := getFloatEnv("FARE_AVERAGE_SPEED_KPH", 30)
	if err != nil {
		return Config{}, fmt.Errorf("invalid FARE_AVERAGE_SPEED_KPH: %w", err)
	}

	cfg := Config{
		ServiceName: getEnv("SERVICE_NAME", "cab-request-handler"),
		HTTPAddr:    getEnv("HTTP_ADDR", ":8082"),
		DB: DBConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     dbPort,
			User:     getEnv("DB_USER", "postgres"),
			Password: getEnv("DB_PASSWORD", "postgres"),
			Name:     getEnv("DB_NAME", "go_ride"),
			SSLMode:  getEnv("DB_SSLMODE", "disable"),
		},
		KafkaBrokers:          splitAndTrim(getEnv("KAFKA_BROKERS", "localhost:9094")),
		KafkaTopic:            getEnv("KAFKA_TOPIC", "ride.requested.v1"),
		DefaultSearchRadiusKM: defaultSearchRadiusKM,
		FareCurrencyCode:      getEnv("FARE_CURRENCY_CODE", "USD"),
		FarePricingVersion:    getEnv("FARE_PRICING_VERSION", "v1"),
		FareBaseAmount:        fareBaseAmount,
		FarePerKmAmount:       farePerKmAmount,
		FarePerMinuteAmount:   farePerMinuteAmount,
		FareMinimumAmount:     fareMinimumAmount,
		FareAverageSpeedKPH:   fareAverageSpeedKPH,
		FareLockTTL:           time.Duration(fareLockTTLMinutes) * time.Minute,
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
	if cfg.FareCurrencyCode == "" {
		return Config{}, fmt.Errorf("FARE_CURRENCY_CODE is required")
	}
	if cfg.FarePricingVersion == "" {
		return Config{}, fmt.Errorf("FARE_PRICING_VERSION is required")
	}
	if cfg.FareLockTTL <= 0 {
		return Config{}, fmt.Errorf("FARE_LOCK_TTL_MINUTES must be greater than zero")
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

func getFloatEnv(key string, fallback float64) (float64, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	return strconv.ParseFloat(value, 64)
}
