package repository

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"b2bpayments/internal/database"
	"b2bpayments/internal/domain"
)

func integrationStore(t *testing.T) *Store {
	t.Helper()
	connectionString := os.Getenv("TEST_DATABASE_URL")
	if connectionString == "" {
		t.Skip("задайте TEST_DATABASE_URL и примените миграции для PostgreSQL integration-тестов")
	}
	pool, err := database.Open(context.Background(), connectionString)
	if err != nil {
		t.Fatalf("подключение к тестовой БД: %v", err)
	}
	t.Cleanup(pool.Close)
	return New(pool)
}

func testEvent(suffix string, eventType string) domain.PaymentEvent {
	now := time.Now().UTC().Truncate(time.Microsecond)
	return domain.PaymentEvent{
		EventID: "it-event-" + suffix, Source: "integration-test", CompanyID: "it-company-" + suffix,
		PaymentID: "it-payment-" + suffix, EventType: eventType, AmountMinor: 12500,
		Currency: "RUB", OccurredAt: now, SchemaVersion: domain.SchemaVersion,
	}
}

func TestSubmitDeduplicatesAndStoresConflict(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	event := testEvent(fmt.Sprint(time.Now().UnixNano()), domain.EventPaymentCreated)
	if _, err := store.SubmitEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	duplicate, err := store.SubmitEvent(ctx, event)
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("повтор не распознан: result=%+v err=%v", duplicate, err)
	}
	conflicting := event
	conflicting.AmountMinor++
	conflict, err := store.SubmitEvent(ctx, conflicting)
	if err != nil || !conflict.Conflict {
		t.Fatalf("конфликт не распознан: result=%+v err=%v", conflict, err)
	}
	var eventCount, outboxCount, conflictCount int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM payment_events WHERE source=$1 AND event_id=$2`, event.Source, event.EventID).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE source=$1 AND event_id=$2`, event.Source, event.EventID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM ingestion_conflicts WHERE source=$1 AND event_id=$2`, event.Source, event.EventID).Scan(&conflictCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 1 || outboxCount != 1 || conflictCount != 1 {
		t.Fatalf("event=%d outbox=%d conflicts=%d", eventCount, outboxCount, conflictCount)
	}
}

func TestConsumerRedeliveryDoesNotRepeatHistoryOrMetrics(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	event := testEvent(fmt.Sprint(time.Now().UnixNano()), domain.EventPaymentCreated)
	if _, err := store.SubmitEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := store.ProcessPaymentEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
		if _, err := store.ProcessAnalyticsEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	var historyCount, paymentMetricsCount, metricEvents int64
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM payment_history WHERE source=$1 AND event_id=$2`, event.Source, event.EventID).Scan(&historyCount); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT total_events FROM company_daily_metrics WHERE company_id=$1 AND metric_date=$2`, event.CompanyID, event.OccurredAt.Format("2006-01-02")).Scan(&metricEvents); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM payments WHERE payment_id=$1`, event.PaymentID).Scan(&paymentMetricsCount); err != nil {
		t.Fatal(err)
	}
	if historyCount != 1 || metricEvents != 1 || paymentMetricsCount != 1 {
		t.Fatalf("history=%d metric_events=%d payments=%d", historyCount, metricEvents, paymentMetricsCount)
	}
}

func TestDifferentEventsForOnePaymentAreProcessedIndependently(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	created := testEvent(fmt.Sprint(time.Now().UnixNano()), domain.EventPaymentCreated)
	if _, err := store.ProcessPaymentEvent(ctx, created); err != nil {
		t.Fatal(err)
	}
	succeeded := created
	succeeded.EventID += "-success"
	succeeded.EventType = domain.EventPaymentSucceeded
	succeeded.OccurredAt = succeeded.OccurredAt.Add(time.Second)
	if _, err := store.ProcessPaymentEvent(ctx, succeeded); err != nil {
		t.Fatal(err)
	}
	payment, err := store.GetPayment(ctx, created.PaymentID)
	if err != nil || payment.Status != domain.StatusSucceeded {
		t.Fatalf("платёж не перешёл в succeeded: payment=%+v err=%v", payment, err)
	}
	var historyCount int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM payment_history WHERE payment_id=$1`, created.PaymentID).Scan(&historyCount); err != nil {
		t.Fatal(err)
	}
	if historyCount != 2 {
		t.Fatalf("ожидалось две записи истории, получено %d", historyCount)
	}
}

func TestFailedConsumerTransactionDoesNotClaimMessage(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	event := testEvent(fmt.Sprint(time.Now().UnixNano()), domain.EventPaymentSucceeded)
	if _, err := store.ProcessPaymentEvent(ctx, event); err == nil {
		t.Fatal("succeeded без payment.created должен завершиться ошибкой")
	}
	var processedCount, eventCount int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM processed_messages WHERE consumer_name='payment-processor' AND source=$1 AND event_id=$2`, event.Source, event.EventID).Scan(&processedCount); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM payment_events WHERE source=$1 AND event_id=$2`, event.Source, event.EventID).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if processedCount != 0 || eventCount != 0 {
		t.Fatalf("часть неуспешной транзакции сохранилась: processed=%d events=%d", processedCount, eventCount)
	}
}

func TestOutboxRemainsPendingUntilMarkedPublished(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	event := testEvent(fmt.Sprint(time.Now().UnixNano()), domain.EventPaymentCreated)
	if _, err := store.SubmitEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	find := func() (OutboxEvent, bool) {
		t.Helper()
		pending, err := store.PendingOutbox(ctx, 10000)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range pending {
			if item.Source == event.Source && item.EventID == event.EventID {
				return item, true
			}
		}
		return OutboxEvent{}, false
	}
	item, found := find()
	if !found {
		t.Fatal("новый Outbox не найден")
	}
	if _, found := find(); !found {
		t.Fatal("повторное чтение до отметки должно позволять повторную публикацию")
	}
	if err := store.MarkOutboxPublishedBatch(ctx, []int64{item.ID}); err != nil {
		t.Fatal(err)
	}
	if _, found := find(); found {
		t.Fatal("опубликованный Outbox всё ещё помечен как ожидающий")
	}
}
