package consumer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"b2bpayments/internal/domain"
	messaging "b2bpayments/internal/kafka"
	kafkago "github.com/segmentio/kafka-go"
)

type Handler func(context.Context, []byte) error

type Runner struct {
	reader    *kafkago.Reader
	dlq       *kafkago.Writer
	handler   Handler
	maxTries  int
	groupName string
	logger    *slog.Logger
}

func NewRunner(brokers []string, topic, group, dlqTopic string, maxTries int, handler Handler, logger *slog.Logger) *Runner {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        group,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        time.Second,
		CommitInterval: 0, // Offset подтверждается явно только после транзакции БД.
	})
	return &Runner{
		reader: reader,
		dlq: kafkago.NewWriter(kafkago.WriterConfig{
			Brokers:      brokers,
			Topic:        dlqTopic,
			Balancer:     &kafkago.Hash{},
			BatchTimeout: messaging.BatchTimeout,
			RequiredAcks: int(kafkago.RequireAll),
		}),
		handler: handler, maxTries: maxTries, groupName: group, logger: logger,
	}
}

func (r *Runner) Close() error {
	readerErr := r.reader.Close()
	writerErr := r.dlq.Close()
	return errors.Join(readerErr, writerErr)
}

func (r *Runner) Run(ctx context.Context) error {
	for {
		message, err := r.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.logger.Error("ошибка чтения Kafka", "consumer_group", r.groupName, "error", err)
			if !wait(ctx, time.Second) {
				return ctx.Err()
			}
			continue
		}
		if err := r.processMessage(ctx, message); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.logger.Error("сообщение не подтверждено; Kafka доставит его повторно", "consumer_group", r.groupName, "partition", message.Partition, "offset", message.Offset, "error", err)
			if !wait(ctx, time.Second) {
				return ctx.Err()
			}
		}
	}
}

func (r *Runner) processMessage(ctx context.Context, message kafkago.Message) error {
	var processingErr error
	for attempt := 1; attempt <= r.maxTries; attempt++ {
		processingErr = r.handler(ctx, message.Value)
		if processingErr == nil {
			return r.commitUntilSuccess(ctx, message)
		}
		r.logger.Warn("ошибка обработки Kafka-сообщения", "consumer_group", r.groupName, "attempt", attempt, "max_attempts", r.maxTries, "error", processingErr)
		if attempt < r.maxTries && !wait(ctx, time.Duration(attempt)*250*time.Millisecond) {
			return ctx.Err()
		}
	}
	deadLetter, err := json.Marshal(struct {
		OriginalTopic string `json:"original_topic"`
		Partition     int    `json:"partition"`
		Offset        int64  `json:"offset"`
		Key           string `json:"key"`
		PayloadBase64 string `json:"payload_base64"`
		Attempts      int    `json:"attempts"`
		Failure       string `json:"failure"`
	}{message.Topic, message.Partition, message.Offset, string(message.Key), base64.StdEncoding.EncodeToString(message.Value), r.maxTries, processingErr.Error()})
	if err != nil {
		return err
	}
	for {
		if err := r.dlq.WriteMessages(ctx, kafkago.Message{Key: message.Key, Value: deadLetter}); err == nil {
			break
		} else {
			r.logger.Error("не удалось записать сообщение в DLQ; offset остаётся неподтверждённым", "consumer_group", r.groupName, "error", err)
			if !wait(ctx, time.Second) {
				return ctx.Err()
			}
		}
	}
	// Source offset подтверждаем только после подтверждённой записи в DLQ.
	return r.commitUntilSuccess(ctx, message)
}

func (r *Runner) commitUntilSuccess(ctx context.Context, message kafkago.Message) error {
	for {
		if err := r.reader.CommitMessages(ctx, message); err == nil {
			return nil
		} else {
			r.logger.Error("не удалось подтвердить Kafka offset; повторяем подтверждение", "consumer_group", r.groupName, "partition", message.Partition, "offset", message.Offset, "error", err)
			if !wait(ctx, time.Second) {
				return ctx.Err()
			}
		}
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func DecodeEvent(payload []byte) (event domain.PaymentEvent, err error) {
	err = json.Unmarshal(payload, &event)
	if err != nil {
		return event, err
	}
	event.Normalize()
	err = event.Validate()
	return event, err
}
