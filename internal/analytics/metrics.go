package analytics

import "b2bpayments/internal/domain"

type DailyMetrics struct {
	TotalEvents      int64
	TotalPayments    int64
	TotalAmountMinor int64
	SucceededCount   int64
	FailedCount      int64
}

// Apply добавляет вклад одного уникального события. Идемпотентный вызов обеспечивает consumer через processed_messages.
func Apply(current DailyMetrics, event domain.PaymentEvent) DailyMetrics {
	current.TotalEvents++
	switch event.EventType {
	case domain.EventPaymentCreated:
		current.TotalPayments++
		current.TotalAmountMinor += event.AmountMinor
	case domain.EventPaymentSucceeded:
		current.SucceededCount++
	case domain.EventPaymentFailed:
		current.FailedCount++
	}
	return current
}
