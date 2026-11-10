package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"b2bpayments/internal/domain"
	"b2bpayments/internal/repository"
)

type mockStore struct {
	submission repository.Submission
	readyErr   error
}

func (m *mockStore) Ready(context.Context) error { return m.readyErr }
func (m *mockStore) SubmitEvent(context.Context, domain.PaymentEvent) (repository.Submission, error) {
	return m.submission, nil
}
func (m *mockStore) GetEvent(context.Context, string, string) (repository.EventRecord, error) {
	return repository.EventRecord{}, repository.ErrNotFound
}
func (m *mockStore) GetPayment(context.Context, string) (repository.Payment, error) {
	return repository.Payment{}, repository.ErrNotFound
}
func (m *mockStore) ListPayments(context.Context, string, string, *time.Time, *time.Time, int, int) ([]repository.Payment, error) {
	return []repository.Payment{}, nil
}
func (m *mockStore) ListDailyMetrics(context.Context, string, time.Time, time.Time) ([]repository.DailyMetric, error) {
	return []repository.DailyMetric{}, nil
}

func TestSubmitEventResponseCodes(t *testing.T) {
	cases := []struct {
		name       string
		submission repository.Submission
		status     int
	}{
		{name: "new", status: http.StatusAccepted},
		{name: "duplicate", submission: repository.Submission{Duplicate: true}, status: http.StatusOK},
		{name: "conflict", submission: repository.Submission{Conflict: true}, status: http.StatusConflict},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			store := &mockStore{submission: test.submission}
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			handler := New(store, logger)
			request := httptest.NewRequest(http.MethodPost, "/api/v1/payment-events", strings.NewReader(`{
				"event_id":"event-1","source":"test","company_id":"company-1","payment_id":"payment-1",
				"event_type":"payment.created","amount_minor":100,"currency":"RUB",
				"occurred_at":"2026-11-03T12:00:00Z","schema_version":1
			}`))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("получен HTTP %d вместо %d: %s", response.Code, test.status, response.Body.String())
			}
		})
	}
}

func TestSubmitEventRejectsInvalidPayload(t *testing.T) {
	store := &mockStore{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := New(store, logger)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/payment-events", strings.NewReader(`{"event_id":"missing-fields"}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("получен HTTP %d вместо 422: %s", response.Code, response.Body.String())
	}
}
