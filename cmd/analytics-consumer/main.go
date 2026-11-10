package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"b2bpayments/internal/config"
	"b2bpayments/internal/consumer"
	"b2bpayments/internal/database"
	"b2bpayments/internal/kafka"
	"b2bpayments/internal/logger"
	"b2bpayments/internal/repository"
)

func main() {
	log := logger.New()
	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("analytics consumer остановлен с ошибкой", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := database.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := kafka.ValidateBrokers(cfg.KafkaBrokers); err != nil {
		return err
	}
	store := repository.New(pool)
	runner := consumer.NewRunner(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.AnalyticsGroup, cfg.KafkaDLQTopic, cfg.ConsumerAttempts, func(ctx context.Context, payload []byte) error {
		event, err := consumer.DecodeEvent(payload)
		if err != nil {
			return err
		}
		_, err = store.ProcessAnalyticsEvent(ctx, event)
		return err
	}, log)
	defer runner.Close()
	log.Info("analytics consumer запущен", "topic", cfg.KafkaTopic, "group", cfg.AnalyticsGroup)
	return runner.Run(ctx)
}
