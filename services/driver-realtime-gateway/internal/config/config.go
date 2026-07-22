package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName string
	HTTPAddr    string
	DB          DBConfig

	KafkaBrokers       []string
	KafkaConsumerGroup string
	OfferCreatedTopic  string

	RedisAddr     string
	RedisPassword string
	RedisDB       int
	RedisChannel  string

	JWTSecret   string
	JWTIssuer   string
	JWTAudience string

	PingInterval time.Duration
	PongWait     time.Duration
	ReplayBatch  int
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

	redisDB, err := getIntEnv("REDIS_DB", 0)
	if err != nil {
		return Config{}, fmt.Errorf("invalid REDIS_DB: %w", err)
	}

	pingIntervalSeconds, err := getIntEnv("WS_PING_INTERVAL_SECONDS", 20)
	if err != nil {
		return Config{}, fmt.Errorf("invalid WS_PING_INTERVAL_SECONDS: %w", err)
	}

	pongWaitSeconds, err := getIntEnv("WS_PONG_WAIT_SECONDS", 60)
	if err != nil {
		return Config{}, fmt.Errorf("invalid WS_PONG_WAIT_SECONDS: %w", err)
	}

	replayBatch, err := getIntEnv("REPLAY_BATCH_SIZE", 50)
	if err != nil {
		return Config{}, fmt.Errorf("invalid REPLAY_BATCH_SIZE: %w", err)
	}

	cfg := Config{
		ServiceName: getEnv("SERVICE_NAME", "driver-realtime-gateway"),
		HTTPAddr:    getEnv("HTTP_ADDR", ":8083"),
		DB: DBConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     dbPort,
			User:     getEnv("DB_USER", "postgres"),
			Password: getEnv("DB_PASSWORD", "postgres"),
			Name:     getEnv("DB_NAME", "go_ride"),
			SSLMode:  getEnv("DB_SSLMODE", "disable"),
		},

		KafkaBrokers:       splitAndTrim(getEnv("KAFKA_BROKERS", "localhost:9094")),
		KafkaConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "driver-realtime-gateway-group"),
		OfferCreatedTopic:  getEnv("KAFKA_OFFER_CREATED_TOPIC", "driver.job_offer.created.v1"),

		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		RedisDB:       redisDB,
		RedisChannel:  getEnv("REDIS_OFFER_CHANNEL", "driver-offers"),

		JWTSecret:   getEnv("JWT_SECRET", ""),
		JWTIssuer:   getEnv("JWT_ISSUER", "go-ride-backend"),
		JWTAudience: getEnv("JWT_AUDIENCE", "go-ride-driver-app"),

		PingInterval: time.Duration(pingIntervalSeconds) * time.Second,
		PongWait:     time.Duration(pongWaitSeconds) * time.Second,
		ReplayBatch:  replayBatch,
	}

	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS is required")
	}
	if cfg.OfferCreatedTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_OFFER_CREATED_TOPIC is required")
	}
	if cfg.RedisAddr == "" {
		return Config{}, fmt.Errorf("REDIS_ADDR is required")
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
	if cfg.PingInterval <= 0 {
		return Config{}, fmt.Errorf("WS_PING_INTERVAL_SECONDS must be greater than zero")
	}
	if cfg.PongWait <= 0 {
		return Config{}, fmt.Errorf("WS_PONG_WAIT_SECONDS must be greater than zero")
	}
	if cfg.ReplayBatch <= 0 {
		return Config{}, fmt.Errorf("REPLAY_BATCH_SIZE must be greater than zero")
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
