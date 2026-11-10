package main

import (
	"testing"
	"time"

	"b2bpayments/internal/domain"
)

func TestMakeRequestsBuildsStablePaymentSequencesAndReplayRatios(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	created, followups, replays, unique, duplicates, conflicts, err := makeRequests(100, 10, 20, 0.1, 0.1, "test-source", now)
	if err != nil {
		t.Fatal(err)
	}
	if unique != 80 || duplicates != 10 || conflicts != 10 || len(created)+len(followups) != unique || len(replays) != 20 {
		t.Fatalf("неверные категории событий: created=%d followups=%d replays=%d unique=%d duplicates=%d conflicts=%d", len(created), len(followups), len(replays), unique, duplicates, conflicts)
	}
	if len(created) != 20 || len(followups) != 60 {
		t.Fatalf("для каждого платежа ожидалось событие создания и два последующих: created=%d followups=%d", len(created), len(followups))
	}
	createdByPayment := make(map[string]domain.PaymentEvent, len(created))
	for _, event := range created {
		createdByPayment[event.PaymentID] = event
	}
	for _, event := range followups {
		original, found := createdByPayment[event.PaymentID]
		if !found || event.EventType != domain.EventPaymentSucceeded || event.AmountMinor != original.AmountMinor || event.CompanyID != original.CompanyID {
			t.Fatalf("последующее событие нарушает идентичность платежа: %+v", event)
		}
	}
}
