package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const SchemaVersion = 1

const (
	EventPaymentCreated   = "payment.created"
	EventPaymentSucceeded = "payment.succeeded"
	EventPaymentFailed    = "payment.failed"
)

// PaymentEvent — неизменяемое событие, передаваемое через API, Outbox и Kafka.
type PaymentEvent struct {
	EventID       string    `json:"event_id"`
	Source        string    `json:"source"`
	CompanyID     string    `json:"company_id"`
	PaymentID     string    `json:"payment_id"`
	EventType     string    `json:"event_type"`
	AmountMinor   int64     `json:"amount_minor"`
	Currency      string    `json:"currency"`
	OccurredAt    time.Time `json:"occurred_at"`
	SchemaVersion int       `json:"schema_version"`
}

func (e *PaymentEvent) Normalize() {
	e.EventID = strings.TrimSpace(e.EventID)
	e.Source = strings.TrimSpace(e.Source)
	e.CompanyID = strings.TrimSpace(e.CompanyID)
	e.PaymentID = strings.TrimSpace(e.PaymentID)
	e.EventType = strings.TrimSpace(e.EventType)
	e.Currency = strings.ToUpper(strings.TrimSpace(e.Currency))
	e.OccurredAt = e.OccurredAt.UTC()
}

func (e PaymentEvent) Validate() error {
	if e.EventID == "" || e.Source == "" || e.CompanyID == "" || e.PaymentID == "" {
		return errors.New("event_id, source, company_id и payment_id обязательны")
	}
	for name, value := range map[string]string{
		"event_id": e.EventID, "source": e.Source, "company_id": e.CompanyID, "payment_id": e.PaymentID,
	} {
		if len(value) > 200 {
			return fmt.Errorf("%s не должен быть длиннее 200 символов", name)
		}
	}
	switch e.EventType {
	case EventPaymentCreated, EventPaymentSucceeded, EventPaymentFailed:
	default:
		return fmt.Errorf("неподдерживаемый event_type: %q", e.EventType)
	}
	if e.AmountMinor <= 0 {
		return errors.New("amount_minor должен быть положительным целым числом")
	}
	if len(e.Currency) != 3 || !isUpperASCII(e.Currency) {
		return errors.New("currency должна состоять из трёх заглавных латинских букв")
	}
	if e.OccurredAt.IsZero() {
		return errors.New("occurred_at обязателен")
	}
	if e.SchemaVersion != SchemaVersion {
		return fmt.Errorf("неподдерживаемая schema_version: %d", e.SchemaVersion)
	}
	return nil
}

func (e PaymentEvent) Hash() (string, error) {
	e.Normalize()
	payload, err := json.Marshal(e)
	if err != nil {
		return "", fmt.Errorf("кодирование события для hash: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (e PaymentEvent) Marshal() ([]byte, error) {
	e.Normalize()
	return json.Marshal(e)
}

func isUpperASCII(value string) bool {
	for _, char := range value {
		if char < 'A' || char > 'Z' {
			return false
		}
	}
	return true
}
