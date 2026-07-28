package config

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/shawon-kanji/go-ride-utils/awssecrets"
	"github.com/shawon-kanji/go-ride-utils/kafkatopics"
)

type Config struct {
	ServiceName    string
	HTTPAddr       string
	DB             DBConfig
	KafkaBrokers   []string
	AssignedTopic  string
	StartedTopic   string
	EndedTopic     string
	CompletedTopic string
	JWTSecret      string
	JWTIssuer      string
	JWTAudience    string
}

type DBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Name     string
	SSLMode  string
}

func Load(ctx context.Context) (Config, error) {
	dbPort, err := getIntEnv("DB_PORT", 5432)
	if err != nil {
		return Config{}, fmt.Errorf("invalid DB_PORT: %w", err)
	}

	dbUser := getEnv("DB_USER", "postgres")
	dbPassword := getEnv("DB_PASSWORD", "postgres")
	if secretName := getEnv("DB_CREDENTIALS_SECRET_NAME", ""); secretName != "" {
		values, err := awssecrets.FetchJSON(ctx, secretName)
		if err != nil {
			return Config{}, fmt.Errorf("fetch db credentials secret: %w", err)
		}
		if v, ok := values["DB_USER"]; ok {
			dbUser = v
		}
		if v, ok := values["DB_PASSWORD"]; ok {
			dbPassword = v
		}
	}

	jwtSecret := getEnv("JWT_SECRET", "")
	if secretName := getEnv("JWT_SECRET_NAME", ""); secretName != "" {
		values, err := awssecrets.FetchJSON(ctx, secretName)
		if err != nil {
			return Config{}, fmt.Errorf("fetch jwt secret: %w", err)
		}
		if v, ok := values["JWT_SECRET"]; ok {
			jwtSecret = v
		}
	}

	cfg := Config{
		ServiceName: getEnv("SERVICE_NAME", "driver-request-handler"),
		HTTPAddr:    getEnv("HTTP_ADDR", ":8084"),
		DB: DBConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     dbPort,
			User:     dbUser,
			Password: dbPassword,
			Name:     getEnv("DB_NAME", "go_ride"),
			SSLMode:  getEnv("DB_SSLMODE", "disable"),
		},
		KafkaBrokers:   splitAndTrim(getEnv("KAFKA_BROKERS", "localhost:9094")),
		AssignedTopic:  getEnv("KAFKA_ASSIGNED_TOPIC", kafkatopics.RideAssignedV1),
		StartedTopic:   getEnv("KAFKA_STARTED_TOPIC", kafkatopics.RideStartedV1),
		EndedTopic:     getEnv("KAFKA_ENDED_TOPIC", kafkatopics.RideEndedV1),
		CompletedTopic: getEnv("KAFKA_COMPLETED_TOPIC", kafkatopics.RideCompletedV1),
		JWTSecret:      jwtSecret,
		JWTIssuer:      getEnv("JWT_ISSUER", "go-ride-backend"),
		JWTAudience:    getEnv("JWT_AUDIENCE", "go-ride-drivers"),
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
	if cfg.EndedTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_ENDED_TOPIC is required")
	}
	if cfg.CompletedTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_COMPLETED_TOPIC is required")
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
