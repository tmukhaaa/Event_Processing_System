package outbox

import (
	"context"
	"log/slog"
	"time"

	"b2bpayments/internal/repository"
	kafkago "github.com/segmentio/kafka-go"
)

type Store interface {
	PendingOutbox(context.Context, int) ([]repository.OutboxEvent, error)
	MarkOutboxPublishedBatch(context.Context, []int64) error
	RecordOutboxFailureBatch(context.Context, []int64, error) error
}

type Publisher struct {
	store    Store
	writer   *kafkago.Writer
	interval time.Duration
	logger   *slog.Logger
}

func New(store Store, writer *kafkago.Writer, interval time.Duration, logger *slog.Logger) *Publisher {
	return &Publisher{store: store, writer: writer, interval: interval, logger: logger}
}

func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		published, err := p.publishPending(ctx)
		if err != nil && ctx.Err() == nil {
			p.logger.Error("ошибка цикла публикации Outbox", "error", err)
		}
		if err == nil && published > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *Publisher) publishPending(ctx context.Context) (int, error) {
	events, err := p.store.PendingOutbox(ctx, 100)
	if err != nil {
		return 0, err
	}
	if len(events) == 0 {
		return 0, nil
	}
	messages := make([]kafkago.Message, 0, len(events))
	ids := make([]int64, 0, len(events))
	for _, event := range events {
		messages = append(messages, kafkago.Message{Key: []byte(event.PaymentID), Value: event.Payload})
		ids = append(ids, event.ID)
	}
	if err := p.writer.WriteMessages(ctx, messages...); err != nil {
		if recordErr := p.store.RecordOutboxFailureBatch(ctx, ids, err); recordErr != nil {
			p.logger.Error("не удалось сохранить ошибку публикации пакета", "count", len(ids), "error", recordErr)
		}
		return 0, err
	}
	if err := p.store.MarkOutboxPublishedBatch(ctx, ids); err != nil {
		// Kafka уже подтвердила пакет: повторная отправка после сбоя безопасна благодаря consumer-дедупликации.
		return 0, err
	}
	p.logger.Info("пакет Outbox опубликован", "count", len(events), "first_outbox_id", events[0].ID, "last_outbox_id", events[len(events)-1].ID)
	return len(events), nil
}
