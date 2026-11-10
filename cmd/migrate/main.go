package main

import (
	"context"
	"fmt"
	"log"

	"b2bpayments/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func main() {
	cfg := config.Load()
	connectionConfig, err := pgx.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("ошибка DATABASE_URL: %v", err)
	}
	db := stdlib.OpenDB(*connectionConfig)
	defer db.Close()
	if err := db.PingContext(context.Background()); err != nil {
		log.Fatalf("подключение к PostgreSQL: %v", err)
	}
	if err := goose.SetDialect("postgres"); err != nil {
		log.Fatalf("настройка goose: %v", err)
	}
	if err := goose.UpContext(context.Background(), db, "migrations"); err != nil {
		log.Fatalf("применение миграций: %v", err)
	}
	fmt.Println("Миграции применены")
}
