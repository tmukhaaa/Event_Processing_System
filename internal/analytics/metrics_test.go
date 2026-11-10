package analytics

import (
	"testing"
	"time"

	"b2bpayments/internal/domain"
)

func TestApplyCountsPaymentsAndOutcomes(t *testing.T) {
	created := domain.PaymentEvent{EventType: domain.EventPaymentCreated, AmountMinor: 2500, OccurredAt: time.Now()}
	succeeded := domain.PaymentEvent{EventType: domain.EventPaymentSucceeded, AmountMinor: 2500, OccurredAt: time.Now()}
	metrics := Apply(DailyMetrics{}, created)
	metrics = Apply(metrics, succeeded)
	if metrics.TotalEvents != 2 || metrics.TotalPayments != 1 || metrics.TotalAmountMinor != 2500 || metrics.SucceededCount != 1 {
		t.Fatalf("неверные агрегаты: %+v", metrics)
	}
}
