package domain

import (
	"testing"
	"time"
)

func TestPaymentEventValidate(t *testing.T) {
	event := validEvent()
	if err := event.Validate(); err != nil {
		t.Fatalf("валидное событие отклонено: %v", err)
	}
	event.AmountMinor = 0
	if err := event.Validate(); err == nil {
		t.Fatal("нулевая сумма должна быть отклонена")
	}
}

func TestPaymentEventHashNormalizesCurrencyAndTime(t *testing.T) {
	first := validEvent()
	first.Currency = "rub"
	second := first
	second.Currency = "RUB"
	second.OccurredAt = second.OccurredAt.In(time.FixedZone("test", 3*60*60))
	firstHash, err := first.Hash()
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := second.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf("нормализованные значения дали разные hash: %s != %s", firstHash, secondHash)
	}
	second.AmountMinor++
	thirdHash, err := second.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if firstHash == thirdHash {
		t.Fatal("изменение содержимого не изменило hash")
	}
}

func validEvent() PaymentEvent {
	return PaymentEvent{EventID: "event-1", Source: "synthetic", CompanyID: "company-1", PaymentID: "payment-1", EventType: EventPaymentCreated, AmountMinor: 1500, Currency: "RUB", OccurredAt: time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC), SchemaVersion: SchemaVersion}
}
