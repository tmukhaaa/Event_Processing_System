package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"b2bpayments/internal/config"
	"b2bpayments/internal/database"
	"b2bpayments/internal/kafka"
	"b2bpayments/internal/logger"
	"b2bpayments/internal/outbox"
	"b2bpayments/internal/repository"
)

func main() {
	log := logger.New()
	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("publisher остановлен с ошибкой", "error", err)
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
	writer := kafka.NewWriter(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer writer.Close()
	publisher := outbox.New(repository.New(pool), writer, cfg.OutboxPollInterval, log)
	log.Info("Outbox publisher запущен", "topic", cfg.KafkaTopic)
	return publisher.Run(ctx)
}
