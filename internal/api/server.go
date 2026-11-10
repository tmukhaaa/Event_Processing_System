package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"b2bpayments/internal/domain"
	"b2bpayments/internal/repository"
)

type store interface {
	Ready(context.Context) error
	SubmitEvent(context.Context, domain.PaymentEvent) (repository.Submission, error)
	GetEvent(context.Context, string, string) (repository.EventRecord, error)
	GetPayment(context.Context, string) (repository.Payment, error)
	ListPayments(context.Context, string, string, *time.Time, *time.Time, int, int) ([]repository.Payment, error)
	ListDailyMetrics(context.Context, string, time.Time, time.Time) ([]repository.DailyMetric, error)
}

type Server struct {
	store  store
	logger *slog.Logger
}

func New(store store, logger *slog.Logger) http.Handler {
	server := &Server{store: store, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.health)
	mux.HandleFunc("GET /readyz", server.ready)
	mux.HandleFunc("POST /api/v1/payment-events", server.submitEvent)
	mux.HandleFunc("GET /api/v1/payment-events/{event_id}", server.getEvent)
	mux.HandleFunc("GET /api/v1/payments/{payment_id}", server.getPayment)
	mux.HandleFunc("GET /api/v1/companies/{company_id}/payments", server.listPayments)
	mux.HandleFunc("GET /api/v1/companies/{company_id}/metrics/daily", server.dailyMetrics)
	return requestLogger(mux, logger)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ready(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "dependency_unavailable", "PostgreSQL недоступен")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) submitEvent(w http.ResponseWriter, r *http.Request) {
	var event domain.PaymentEvent
	if err := decodeJSON(w, r, &event); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	event.Normalize()
	if err := event.Validate(); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "invalid_event", err.Error())
		return
	}
	result, err := s.store.SubmitEvent(r.Context(), event)
	if err != nil {
		s.logger.Error("ошибка приёма события", "event_id", event.EventID, "error", err)
		writeError(w, http.StatusInternalServerError, "storage_error", "не удалось сохранить событие")
		return
	}
	if result.Conflict {
		writeError(w, http.StatusConflict, "event_id_conflict", "event_id уже принят с другим содержимым")
		return
	}
	if result.Duplicate {
		writeJSON(w, http.StatusOK, map[string]any{"status": "duplicate", "event_id": event.EventID})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "accepted", "event_id": event.EventID})
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("event_id"))
	if eventID == "" {
		writeError(w, http.StatusBadRequest, "invalid_event_id", "event_id обязателен")
		return
	}
	record, err := s.store.GetEvent(r.Context(), eventID, strings.TrimSpace(r.URL.Query().Get("source")))
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "событие не найдено")
		return
	}
	if errors.Is(err, repository.ErrAmbiguous) {
		writeError(w, http.StatusConflict, "ambiguous_event_id", err.Error())
		return
	}
	if err != nil {
		s.logger.Error("ошибка поиска события", "event_id", eventID, "error", err)
		writeError(w, http.StatusInternalServerError, "storage_error", "не удалось получить событие")
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (s *Server) getPayment(w http.ResponseWriter, r *http.Request) {
	paymentID := strings.TrimSpace(r.PathValue("payment_id"))
	if paymentID == "" {
		writeError(w, http.StatusBadRequest, "invalid_payment_id", "payment_id обязателен")
		return
	}
	payment, err := s.store.GetPayment(r.Context(), paymentID)
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "платёж не найден")
		return
	}
	if err != nil {
		s.logger.Error("ошибка поиска платежа", "payment_id", paymentID, "error", err)
		writeError(w, http.StatusInternalServerError, "storage_error", "не удалось получить платёж")
		return
	}
	writeJSON(w, http.StatusOK, payment)
}

func (s *Server) listPayments(w http.ResponseWriter, r *http.Request) {
	companyID := strings.TrimSpace(r.PathValue("company_id"))
	if companyID == "" {
		writeError(w, http.StatusBadRequest, "invalid_company_id", "company_id обязателен")
		return
	}
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status != "" && status != domain.StatusCreated && status != domain.StatusSucceeded && status != domain.StatusFailed {
		writeError(w, http.StatusBadRequest, "invalid_status", "status должен быть created, succeeded или failed")
		return
	}
	from, err := optionalTime(r.URL.Query().Get("from"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_from", "from должен быть временем RFC3339")
		return
	}
	to, err := optionalTime(r.URL.Query().Get("to"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_to", "to должен быть временем RFC3339")
		return
	}
	if from != nil && to != nil && !from.Before(*to) {
		writeError(w, http.StatusBadRequest, "invalid_period", "from должен быть раньше to")
		return
	}
	limit, offset, err := pagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_pagination", err.Error())
		return
	}
	items, err := s.store.ListPayments(r.Context(), companyID, status, from, to, limit, offset)
	if err != nil {
		s.logger.Error("ошибка списка платежей", "company_id", companyID, "error", err)
		writeError(w, http.StatusInternalServerError, "storage_error", "не удалось получить платежи")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "limit": limit, "offset": offset})
}

func (s *Server) dailyMetrics(w http.ResponseWriter, r *http.Request) {
	companyID := strings.TrimSpace(r.PathValue("company_id"))
	if companyID == "" {
		writeError(w, http.StatusBadRequest, "invalid_company_id", "company_id обязателен")
		return
	}
	to := time.Now().UTC().Truncate(24 * time.Hour)
	from := to.AddDate(0, 0, -29)
	if value := r.URL.Query().Get("from"); value != "" {
		parsed, err := time.Parse("2006-01-02", value)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_from", "from должен быть датой в формате YYYY-MM-DD")
			return
		}
		from = parsed.UTC()
	}
	if value := r.URL.Query().Get("to"); value != "" {
		parsed, err := time.Parse("2006-01-02", value)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_to", "to должен быть датой в формате YYYY-MM-DD")
			return
		}
		to = parsed.UTC()
	}
	if from.After(to) {
		writeError(w, http.StatusBadRequest, "invalid_period", "from не может быть позже to")
		return
	}
	metrics, err := s.store.ListDailyMetrics(r.Context(), companyID, from, to)
	if err != nil {
		s.logger.Error("ошибка получения дневной статистики", "company_id", companyID, "error", err)
		writeError(w, http.StatusInternalServerError, "storage_error", "не удалось получить статистику")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": metrics, "from": from.Format("2006-01-02"), "to": to.Format("2006-01-02")})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("некорректное JSON-тело: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("тело должно содержать ровно один JSON-объект")
	}
	return nil
}

type apiError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	var response apiError
	response.Error.Code = code
	response.Error.Message = message
	writeJSON(w, status, response)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		// После WriteHeader изменить статус уже нельзя; ошибка записи остаётся в контексте запроса.
		return
	}
}

func optionalTime(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func pagination(r *http.Request) (int, int, error) {
	limit, offset := 100, 0
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 1000 {
			return 0, 0, errors.New("limit должен быть от 1 до 1000")
		}
		limit = parsed
	}
	if value := r.URL.Query().Get("offset"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			return 0, 0, errors.New("offset должен быть неотрицательным целым числом")
		}
		offset = parsed
	}
	return limit, offset, nil
}

func requestLogger(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		logger.Info("HTTP-запрос", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(started).Milliseconds())
	})
}
