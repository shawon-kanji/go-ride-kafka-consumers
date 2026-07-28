package config

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/shawon-kanji/go-ride-utils/awssecrets"
	"github.com/shawon-kanji/go-ride-utils/kafkatopics"
)

type Config struct {
	ServiceName string
	HTTPAddr    string
	DB          DBConfig

	KafkaBrokers       []string
	KafkaConsumerGroup string
	OfferCreatedTopic  string
	RideAssignedTopic  string
	RideStartedTopic   string
	RideEndedTopic     string
	RideCompletedTopic string
	// DriverLocationTopic must match location-producers' KAFKA_TOPIC / the
	// root Makefile's LOCATION_TOPIC — they must stay in sync manually,
	// same caveat as DriverCommissionRate below.
	DriverLocationTopic string

	RedisAddr                 string
	RedisPassword             string
	RedisDB                   int
	RedisChannel              string
	RedisAssignmentChannel    string
	RedisLocationChannel      string
	RedisTripStartedChannel   string
	RedisTripEndedChannel     string
	RedisTripCompletedChannel string

	// ActiveTripTTL bounds how long a driver->rider active-trip mapping
	// (used to filter the location firehose) survives in Redis. There's no
	// trip-completion/cancellation event yet to clear it early, so this TTL
	// is the sole "stop forwarding" mechanism for now.
	ActiveTripTTL time.Duration

	JWTSecret   string
	JWTIssuer   string
	JWTAudience string

	PingInterval time.Duration
	PongWait     time.Duration
	ReplayBatch  int

	// DriverCommissionRate must match trip-dispatch-worker's
	// DRIVER_COMMISSION_RATE, since replayed offers recompute the same
	// estimated_earning that the live push already carried on the
	// (denormalized) Kafka event.
	DriverCommissionRate float64
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

	driverCommissionRate, err := getFloatEnv("DRIVER_COMMISSION_RATE", 0.20)
	if err != nil {
		return Config{}, fmt.Errorf("invalid DRIVER_COMMISSION_RATE: %w", err)
	}

	activeTripTTLSeconds, err := getIntEnv("ACTIVE_TRIP_TTL_SECONDS", 7200)
	if err != nil {
		return Config{}, fmt.Errorf("invalid ACTIVE_TRIP_TTL_SECONDS: %w", err)
	}

	cfg := Config{
		ServiceName: getEnv("SERVICE_NAME", "websocket-gateway"),
		HTTPAddr:    getEnv("HTTP_ADDR", ":8083"),
		DB: DBConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     dbPort,
			User:     dbUser,
			Password: dbPassword,
			Name:     getEnv("DB_NAME", "go_ride"),
			SSLMode:  getEnv("DB_SSLMODE", "disable"),
		},

		KafkaBrokers:        splitAndTrim(getEnv("KAFKA_BROKERS", "localhost:9094")),
		KafkaConsumerGroup:  getEnv("KAFKA_CONSUMER_GROUP", "websocket-gateway-group"),
		OfferCreatedTopic:   getEnv("KAFKA_OFFER_CREATED_TOPIC", kafkatopics.DriverJobOfferCreatedV1),
		RideAssignedTopic:   getEnv("KAFKA_ASSIGNED_TOPIC", kafkatopics.RideAssignedV1),
		RideStartedTopic:    getEnv("KAFKA_STARTED_TOPIC", kafkatopics.RideStartedV1),
		RideEndedTopic:      getEnv("KAFKA_ENDED_TOPIC", kafkatopics.RideEndedV1),
		RideCompletedTopic:  getEnv("KAFKA_COMPLETED_TOPIC", kafkatopics.RideCompletedV1),
		DriverLocationTopic: getEnv("KAFKA_LOCATION_TOPIC", kafkatopics.DriverLocationUpdatedV1),

		RedisAddr:                 getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:             getEnv("REDIS_PASSWORD", ""),
		RedisDB:                   redisDB,
		RedisChannel:              getEnv("REDIS_OFFER_CHANNEL", "driver-offers"),
		RedisAssignmentChannel:    getEnv("REDIS_ASSIGNMENT_CHANNEL", "ride-assignments"),
		RedisLocationChannel:      getEnv("REDIS_LOCATION_CHANNEL", "driver-location-updates"),
		RedisTripStartedChannel:   getEnv("REDIS_TRIP_STARTED_CHANNEL", "trip-started"),
		RedisTripEndedChannel:     getEnv("REDIS_TRIP_ENDED_CHANNEL", "trip-ended"),
		RedisTripCompletedChannel: getEnv("REDIS_TRIP_COMPLETED_CHANNEL", "trip-completed"),

		ActiveTripTTL: time.Duration(activeTripTTLSeconds) * time.Second,

		JWTSecret:   jwtSecret,
		JWTIssuer:   getEnv("JWT_ISSUER", "go-ride-backend"),
		JWTAudience: getEnv("JWT_AUDIENCE", "go-ride-driver-app"),

		PingInterval: time.Duration(pingIntervalSeconds) * time.Second,
		PongWait:     time.Duration(pongWaitSeconds) * time.Second,
		ReplayBatch:  replayBatch,

		DriverCommissionRate: driverCommissionRate,
	}

	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS is required")
	}
	if cfg.OfferCreatedTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_OFFER_CREATED_TOPIC is required")
	}
	if cfg.RideAssignedTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_ASSIGNED_TOPIC is required")
	}
	if cfg.RideStartedTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_STARTED_TOPIC is required")
	}
	if cfg.RideEndedTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_ENDED_TOPIC is required")
	}
	if cfg.RideCompletedTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_COMPLETED_TOPIC is required")
	}
	if cfg.DriverLocationTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_LOCATION_TOPIC is required")
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
	if cfg.DriverCommissionRate < 0 || cfg.DriverCommissionRate >= 1 {
		return Config{}, fmt.Errorf("DRIVER_COMMISSION_RATE must be in [0, 1)")
	}
	if cfg.ActiveTripTTL <= 0 {
		return Config{}, fmt.Errorf("ACTIVE_TRIP_TTL_SECONDS must be greater than zero")
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
