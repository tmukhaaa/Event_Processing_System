package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr           string
	DatabaseURL        string
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaDLQTopic      string
	PaymentGroup       string
	AnalyticsGroup     string
	OutboxPollInterval time.Duration
	ConsumerAttempts   int
}

func Load() Config {
	return Config{
		HTTPAddr:           env("HTTP_ADDR", ":8080"),
		DatabaseURL:        env("DATABASE_URL", "postgres://payment:payment_dev_password@localhost:5432/payment?sslmode=disable"),
		KafkaBrokers:       split(env("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:         env("KAFKA_TOPIC", "payment-events"),
		KafkaDLQTopic:      env("KAFKA_DLQ_TOPIC", "payment-events-dlq"),
		PaymentGroup:       env("PAYMENT_CONSUMER_GROUP", "payment-processor"),
		AnalyticsGroup:     env("ANALYTICS_CONSUMER_GROUP", "payment-analytics"),
		OutboxPollInterval: duration("OUTBOX_POLL_INTERVAL", time.Second),
		ConsumerAttempts:   integer("CONSUMER_MAX_ATTEMPTS", 3),
	}
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func split(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func duration(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(env(key, fallback.String()))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func integer(key string, fallback int) int {
	value, err := strconv.Atoi(env(key, strconv.Itoa(fallback)))
	if err != nil || value < 1 {
		return fallback
	}
	return value
}
