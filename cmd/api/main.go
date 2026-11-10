package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"b2bpayments/internal/api"
	"b2bpayments/internal/config"
	"b2bpayments/internal/database"
	"b2bpayments/internal/logger"
	"b2bpayments/internal/repository"
)

func main() {
	log := logger.New()
	if err := run(log); err != nil {
		log.Error("API остановлен с ошибкой", "error", err)
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
	server := &http.Server{
		Addr: cfg.HTTPAddr, Handler: api.New(repository.New(pool), log),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()
	log.Info("HTTP API запущен", "address", cfg.HTTPAddr)
	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}
