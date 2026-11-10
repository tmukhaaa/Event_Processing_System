package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"b2bpayments/internal/deduplication"
	"b2bpayments/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound  = errors.New("запись не найдена")
	ErrAmbiguous = errors.New("event_id найден в нескольких источниках; укажите source")
)

type Store struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

type Submission struct {
	Duplicate bool
	Conflict  bool
}

type EventRecord struct {
	Event      domain.PaymentEvent `json:"event"`
	ReceivedAt time.Time           `json:"received_at"`
}

type Payment struct {
	PaymentID   string    `json:"payment_id"`
	CompanyID   string    `json:"company_id"`
	AmountMinor int64     `json:"amount_minor"`
	Currency    string    `json:"currency"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type DailyMetric struct {
	CompanyID        string    `json:"company_id"`
	MetricDate       time.Time `json:"metric_date"`
	TotalEvents      int64     `json:"total_events"`
	TotalPayments    int64     `json:"total_payments"`
	TotalAmountMinor int64     `json:"total_amount_minor"`
	SucceededCount   int64     `json:"succeeded_count"`
	FailedCount      int64     `json:"failed_count"`
}

type OutboxEvent struct {
	ID        int64
	Source    string
	EventID   string
	PaymentID string
	Payload   []byte
}

func (s *Store) Ready(ctx context.Context) error { return s.pool.Ping(ctx) }

// SubmitEvent атомарно сохраняет принятое событие и его запись Outbox.
func (s *Store) SubmitEvent(ctx context.Context, event domain.PaymentEvent) (Submission, error) {
	event.Normalize()
	if err := event.Validate(); err != nil {
		return Submission{}, err
	}
	hash, err := event.Hash()
	if err != nil {
		return Submission{}, err
	}
	payload, err := event.Marshal()
	if err != nil {
		return Submission{}, fmt.Errorf("кодирование события: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Submission{}, fmt.Errorf("начало транзакции приёма: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := ensureCompany(ctx, tx, event.CompanyID); err != nil {
		return Submission{}, err
	}
	// Сериализуем приём событий одного платежа: это сохраняет порядок их Outbox id при параллельных HTTP-запросах.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, event.PaymentID); err != nil {
		return Submission{}, fmt.Errorf("блокировка порядка событий платежа: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO payment_events
		(event_id, source, company_id, payment_id, event_type, amount_minor, currency, occurred_at, schema_version, payload, payload_hash)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (source, event_id) DO NOTHING`,
		event.EventID, event.Source, event.CompanyID, event.PaymentID, event.EventType,
		event.AmountMinor, event.Currency, event.OccurredAt, event.SchemaVersion, payload, hash)
	if err != nil {
		return Submission{}, fmt.Errorf("сохранение payment_events: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var existingHash string
		if err := tx.QueryRow(ctx, `SELECT payload_hash FROM payment_events WHERE source=$1 AND event_id=$2`, event.Source, event.EventID).Scan(&existingHash); err != nil {
			return Submission{}, fmt.Errorf("чтение принятого события: %w", err)
		}
		if deduplication.Classify(existingHash, hash) == deduplication.Duplicate {
			if err := tx.Commit(ctx); err != nil {
				return Submission{}, fmt.Errorf("завершение транзакции дубликата: %w", err)
			}
			return Submission{Duplicate: true}, nil
		}
		if _, err := tx.Exec(ctx, `INSERT INTO ingestion_conflicts (source, event_id, incoming_hash, incoming_payload) VALUES ($1,$2,$3,$4)`, event.Source, event.EventID, hash, payload); err != nil {
			return Submission{}, fmt.Errorf("фиксация конфликта события: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return Submission{}, fmt.Errorf("завершение транзакции конфликта: %w", err)
		}
		return Submission{Conflict: true}, nil
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox_events (source, event_id, payment_id, payload) VALUES ($1,$2,$3,$4)`, event.Source, event.EventID, event.PaymentID, payload); err != nil {
		return Submission{}, fmt.Errorf("создание Outbox-события: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Submission{}, fmt.Errorf("завершение транзакции приёма: %w", err)
	}
	return Submission{}, nil
}

func (s *Store) GetEvent(ctx context.Context, eventID, source string) (EventRecord, error) {
	query := `SELECT payload, received_at FROM payment_events WHERE event_id=$1`
	args := []any{eventID}
	if source != "" {
		query += ` AND source=$2`
		args = append(args, source)
	}
	query += ` ORDER BY received_at DESC LIMIT 2`
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return EventRecord{}, fmt.Errorf("поиск события: %w", err)
	}
	defer rows.Close()
	var result EventRecord
	count := 0
	for rows.Next() {
		count++
		if count > 1 {
			return EventRecord{}, ErrAmbiguous
		}
		var payload []byte
		if err := rows.Scan(&payload, &result.ReceivedAt); err != nil {
			return EventRecord{}, fmt.Errorf("чтение события: %w", err)
		}
		if err := json.Unmarshal(payload, &result.Event); err != nil {
			return EventRecord{}, fmt.Errorf("декодирование сохранённого события: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return EventRecord{}, fmt.Errorf("чтение результатов поиска события: %w", err)
	}
	if count == 0 {
		return EventRecord{}, ErrNotFound
	}
	return result, nil
}

func (s *Store) GetPayment(ctx context.Context, paymentID string) (Payment, error) {
	var payment Payment
	err := s.pool.QueryRow(ctx, `
		SELECT payment_id, company_id, amount_minor, currency, status, created_at, updated_at
		FROM payments WHERE payment_id=$1`, paymentID).Scan(
		&payment.PaymentID, &payment.CompanyID, &payment.AmountMinor, &payment.Currency,
		&payment.Status, &payment.CreatedAt, &payment.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, ErrNotFound
	}
	if err != nil {
		return Payment{}, fmt.Errorf("поиск платежа: %w", err)
	}
	return payment, nil
}

func (s *Store) ListPayments(ctx context.Context, companyID, status string, from, to *time.Time, limit, offset int) ([]Payment, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT payment_id, company_id, amount_minor, currency, status, created_at, updated_at
		FROM payments
		WHERE company_id=$1 AND ($2='' OR status=$2)
		  AND ($3::timestamptz IS NULL OR created_at >= $3)
		  AND ($4::timestamptz IS NULL OR created_at < $4)
		ORDER BY created_at DESC, payment_id
		LIMIT $5 OFFSET $6`, companyID, status, from, to, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("список платежей: %w", err)
	}
	defer rows.Close()
	payments := make([]Payment, 0)
	for rows.Next() {
		var payment Payment
		if err := rows.Scan(&payment.PaymentID, &payment.CompanyID, &payment.AmountMinor, &payment.Currency, &payment.Status, &payment.CreatedAt, &payment.UpdatedAt); err != nil {
			return nil, fmt.Errorf("чтение платежа: %w", err)
		}
		payments = append(payments, payment)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("чтение списка платежей: %w", err)
	}
	return payments, nil
}

func (s *Store) ListDailyMetrics(ctx context.Context, companyID string, from, to time.Time) ([]DailyMetric, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT company_id, metric_date, total_events, total_payments, total_amount_minor, succeeded_count, failed_count
		FROM company_daily_metrics WHERE company_id=$1 AND metric_date >= $2 AND metric_date <= $3
		ORDER BY metric_date`, companyID, from, to)
	if err != nil {
		return nil, fmt.Errorf("получение дневной статистики: %w", err)
	}
	defer rows.Close()
	metrics := make([]DailyMetric, 0)
	for rows.Next() {
		var metric DailyMetric
		if err := rows.Scan(&metric.CompanyID, &metric.MetricDate, &metric.TotalEvents, &metric.TotalPayments, &metric.TotalAmountMinor, &metric.SucceededCount, &metric.FailedCount); err != nil {
			return nil, fmt.Errorf("чтение дневной статистики: %w", err)
		}
		metrics = append(metrics, metric)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("чтение результатов статистики: %w", err)
	}
	return metrics, nil
}

func (s *Store) PendingOutbox(ctx context.Context, limit int) ([]OutboxEvent, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, source, event_id, payment_id, payload FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("чтение Outbox: %w", err)
	}
	defer rows.Close()
	result := make([]OutboxEvent, 0)
	for rows.Next() {
		var event OutboxEvent
		if err := rows.Scan(&event.ID, &event.Source, &event.EventID, &event.PaymentID, &event.Payload); err != nil {
			return nil, fmt.Errorf("чтение записи Outbox: %w", err)
		}
		result = append(result, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("чтение записей Outbox: %w", err)
	}
	return result, nil
}

func (s *Store) MarkOutboxPublishedBatch(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events SET published_at=now(), last_error=NULL WHERE id=ANY($1::bigint[]) AND published_at IS NULL`, ids)
	if err != nil {
		return fmt.Errorf("отметка пакета Outbox как опубликованного: %w", err)
	}
	return nil
}

func (s *Store) RecordOutboxFailureBatch(ctx context.Context, ids []int64, cause error) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events SET attempts=attempts+1, last_error=$2 WHERE id=ANY($1::bigint[]) AND published_at IS NULL`, ids, cause.Error())
	if err != nil {
		return fmt.Errorf("сохранение ошибки публикации пакета Outbox: %w", err)
	}
	return nil
}

func (s *Store) ProcessPaymentEvent(ctx context.Context, event domain.PaymentEvent) (bool, error) {
	event.Normalize()
	if err := event.Validate(); err != nil {
		return false, err
	}
	hash, err := event.Hash()
	if err != nil {
		return false, err
	}
	payload, err := event.Marshal()
	if err != nil {
		return false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("начало транзакции consumer: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := ensureCompany(ctx, tx, event.CompanyID); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO payment_events
		(event_id, source, company_id, payment_id, event_type, amount_minor, currency, occurred_at, schema_version, payload, payload_hash)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (source,event_id) DO NOTHING`,
		event.EventID, event.Source, event.CompanyID, event.PaymentID, event.EventType, event.AmountMinor, event.Currency, event.OccurredAt, event.SchemaVersion, payload, hash); err != nil {
		return false, fmt.Errorf("обеспечение события в БД consumer: %w", err)
	}
	var acceptedHash string
	if err := tx.QueryRow(ctx, `SELECT payload_hash FROM payment_events WHERE source=$1 AND event_id=$2`, event.Source, event.EventID).Scan(&acceptedHash); err != nil {
		return false, fmt.Errorf("проверка принятого события consumer: %w", err)
	}
	if deduplication.Classify(acceptedHash, hash) == deduplication.Conflict {
		return false, errors.New("Kafka-сообщение конфликтует с ранее принятым событием")
	}
	tag, err := tx.Exec(ctx, `INSERT INTO processed_messages (consumer_name, source, event_id, payload_hash) VALUES ('payment-processor',$1,$2,$3) ON CONFLICT DO NOTHING`, event.Source, event.EventID, hash)
	if err != nil {
		return false, fmt.Errorf("регистрация обработанного сообщения: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var processedHash string
		if err := tx.QueryRow(ctx, `SELECT payload_hash FROM processed_messages WHERE consumer_name='payment-processor' AND source=$1 AND event_id=$2`, event.Source, event.EventID).Scan(&processedHash); err != nil {
			return false, fmt.Errorf("проверка повторного сообщения: %w", err)
		}
		if deduplication.Classify(processedHash, hash) == deduplication.Conflict {
			return false, errors.New("повторно доставлено сообщение с тем же ключом и другим содержимым")
		}
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("фиксация повторной обработки: %w", err)
		}
		return true, nil
	}
	desiredStatus, _ := domain.StatusForEvent(event.EventType)
	var existingCompany, existingCurrency, currentStatus string
	var existingAmount int64
	err = tx.QueryRow(ctx, `SELECT company_id, amount_minor, currency, status FROM payments WHERE payment_id=$1 FOR UPDATE`, event.PaymentID).Scan(&existingCompany, &existingAmount, &existingCurrency, &currentStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		if desiredStatus != domain.StatusCreated {
			return false, fmt.Errorf("недопустимый переход: платёж %q отсутствует перед статусом %q", event.PaymentID, desiredStatus)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO payments (payment_id, company_id, amount_minor, currency, status, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$6)`, event.PaymentID, event.CompanyID, event.AmountMinor, event.Currency, desiredStatus, event.OccurredAt); err != nil {
			return false, fmt.Errorf("создание платежа: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO payment_history (payment_id, source, event_id, from_status, to_status, occurred_at) VALUES ($1,$2,$3,NULL,$4,$5)`, event.PaymentID, event.Source, event.EventID, desiredStatus, event.OccurredAt); err != nil {
			return false, fmt.Errorf("запись истории создания платежа: %w", err)
		}
	} else if err != nil {
		return false, fmt.Errorf("чтение текущего платежа: %w", err)
	} else {
		if existingCompany != event.CompanyID || existingAmount != event.AmountMinor || existingCurrency != event.Currency {
			return false, fmt.Errorf("событие не совпадает с финансовыми атрибутами платежа %q", event.PaymentID)
		}
		if currentStatus != desiredStatus {
			if !domain.CanTransition(currentStatus, desiredStatus) {
				return false, fmt.Errorf("недопустимый переход статуса платежа %q: %s -> %s", event.PaymentID, currentStatus, desiredStatus)
			}
			if _, err := tx.Exec(ctx, `UPDATE payments SET status=$2, updated_at=$3 WHERE payment_id=$1`, event.PaymentID, desiredStatus, event.OccurredAt); err != nil {
				return false, fmt.Errorf("обновление статуса платежа: %w", err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO payment_history (payment_id, source, event_id, from_status, to_status, occurred_at) VALUES ($1,$2,$3,$4,$5,$6)`, event.PaymentID, event.Source, event.EventID, currentStatus, desiredStatus, event.OccurredAt); err != nil {
				return false, fmt.Errorf("запись истории статуса платежа: %w", err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("фиксация транзакции consumer: %w", err)
	}
	return false, nil
}

func (s *Store) ProcessAnalyticsEvent(ctx context.Context, event domain.PaymentEvent) (bool, error) {
	event.Normalize()
	if err := event.Validate(); err != nil {
		return false, err
	}
	hash, err := event.Hash()
	if err != nil {
		return false, err
	}
	payload, err := event.Marshal()
	if err != nil {
		return false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("начало транзакции аналитики: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := ensureCompany(ctx, tx, event.CompanyID); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO payment_events
		(event_id, source, company_id, payment_id, event_type, amount_minor, currency, occurred_at, schema_version, payload, payload_hash)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (source,event_id) DO NOTHING`,
		event.EventID, event.Source, event.CompanyID, event.PaymentID, event.EventType, event.AmountMinor, event.Currency, event.OccurredAt, event.SchemaVersion, payload, hash); err != nil {
		return false, fmt.Errorf("обеспечение события в БД аналитики: %w", err)
	}
	var acceptedHash string
	if err := tx.QueryRow(ctx, `SELECT payload_hash FROM payment_events WHERE source=$1 AND event_id=$2`, event.Source, event.EventID).Scan(&acceptedHash); err != nil {
		return false, fmt.Errorf("проверка события аналитики: %w", err)
	}
	if deduplication.Classify(acceptedHash, hash) == deduplication.Conflict {
		return false, errors.New("аналитика получила конфликтующее содержимое события")
	}
	tag, err := tx.Exec(ctx, `INSERT INTO processed_messages (consumer_name, source, event_id, payload_hash) VALUES ('payment-analytics',$1,$2,$3) ON CONFLICT DO NOTHING`, event.Source, event.EventID, hash)
	if err != nil {
		return false, fmt.Errorf("регистрация аналитического сообщения: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var processedHash string
		if err := tx.QueryRow(ctx, `SELECT payload_hash FROM processed_messages WHERE consumer_name='payment-analytics' AND source=$1 AND event_id=$2`, event.Source, event.EventID).Scan(&processedHash); err != nil {
			return false, fmt.Errorf("проверка повторного сообщения аналитики: %w", err)
		}
		if deduplication.Classify(processedHash, hash) == deduplication.Conflict {
			return false, errors.New("аналитика повторно получила тот же ключ с другим содержимым")
		}
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("фиксация дубликата аналитики: %w", err)
		}
		return true, nil
	}
	var totalPayments, amountMinor, succeeded, failed int64
	if event.EventType == domain.EventPaymentCreated {
		totalPayments, amountMinor = 1, event.AmountMinor
	}
	if event.EventType == domain.EventPaymentSucceeded {
		succeeded = 1
	}
	if event.EventType == domain.EventPaymentFailed {
		failed = 1
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO company_daily_metrics (company_id, metric_date, total_events, total_payments, total_amount_minor, succeeded_count, failed_count)
		VALUES ($1,$2,1,$3,$4,$5,$6)
		ON CONFLICT (company_id, metric_date) DO UPDATE SET
			total_events=company_daily_metrics.total_events+1,
			total_payments=company_daily_metrics.total_payments+EXCLUDED.total_payments,
			total_amount_minor=company_daily_metrics.total_amount_minor+EXCLUDED.total_amount_minor,
			succeeded_count=company_daily_metrics.succeeded_count+EXCLUDED.succeeded_count,
			failed_count=company_daily_metrics.failed_count+EXCLUDED.failed_count,
			updated_at=now()`, event.CompanyID, event.OccurredAt.Format("2006-01-02"), totalPayments, amountMinor, succeeded, failed)
	if err != nil {
		return false, fmt.Errorf("обновление дневной статистики: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("фиксация транзакции аналитики: %w", err)
	}
	return false, nil
}

func ensureCompany(ctx context.Context, tx pgx.Tx, companyID string) error {
	if _, err := tx.Exec(ctx, `INSERT INTO companies (company_id, display_name) VALUES ($1,$1) ON CONFLICT (company_id) DO NOTHING`, companyID); err != nil {
		return fmt.Errorf("создание компании: %w", err)
	}
	return nil
}
