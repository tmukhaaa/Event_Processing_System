package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"b2bpayments/internal/domain"
)

type report struct {
	StartedAt         time.Time `json:"started_at"`
	FinishedAt        time.Time `json:"finished_at"`
	Endpoint          string    `json:"endpoint"`
	Source            string    `json:"source"`
	Requested         int       `json:"requested"`
	Unique            int       `json:"unique"`
	Duplicates        int       `json:"duplicates"`
	Conflicts         int       `json:"conflicts"`
	Workers           int       `json:"workers"`
	Accepted          int       `json:"accepted"`
	DuplicateReplies  int       `json:"duplicate_replies"`
	ConflictReplies   int       `json:"conflict_replies"`
	Errors            int       `json:"errors"`
	DurationMillis    int64     `json:"duration_ms"`
	RequestsPerSecond float64   `json:"requests_per_second"`
	MeanLatencyMillis float64   `json:"mean_latency_ms"`
	P95LatencyMillis  float64   `json:"p95_latency_ms"`
	P99LatencyMillis  float64   `json:"p99_latency_ms"`
	ConsumerLag       *int64    `json:"consumer_lag,omitempty"`
	LagNote           string    `json:"lag_note"`
}

type outcome struct {
	status  int
	latency time.Duration
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	endpoint := flag.String("url", "http://localhost:8080", "базовый URL API")
	total := flag.Int("events", 1000, "общее число HTTP-запросов")
	companies := flag.Int("companies", 10, "число синтетических компаний")
	payments := flag.Int("payments", 1000, "число идентификаторов платежей")
	source := flag.String("source", "", "идентификатор источника; по умолчанию генерируется для запуска")
	duplicateRate := flag.Float64("duplicate-rate", 0, "доля полных дубликатов от общего числа запросов, от 0 до 1")
	conflictRate := flag.Float64("conflict-rate", 0, "доля конфликтов event_id от общего числа запросов, от 0 до 1")
	workers := flag.Int("workers", 16, "число параллельных запросов")
	output := flag.String("output", "results/loadgen-latest.json", "путь для JSON-отчёта")
	flag.Parse()
	if *total < 1 || *companies < 1 || *payments < 1 || *workers < 1 {
		return errors.New("events, companies, payments и workers должны быть положительными")
	}
	if *duplicateRate < 0 || *conflictRate < 0 || *duplicateRate+*conflictRate >= 1 {
		return errors.New("сумма duplicate-rate и conflict-rate должна быть от 0 включительно до 1 не включительно")
	}
	baseURL := strings.TrimRight(*endpoint, "/") + "/api/v1/payment-events"
	runID := time.Now().UTC()
	runSource := *source
	if runSource == "" {
		runSource = fmt.Sprintf("loadgen-%d", runID.UnixNano())
	}
	createdEvents, followupEvents, replayEvents, uniqueCount, duplicateCount, conflictCount, err := makeRequests(*total, *companies, *payments, *duplicateRate, *conflictRate, runSource, runID)
	if err != nil {
		return err
	}
	started := time.Now().UTC()
	reportValue := report{StartedAt: started, Endpoint: baseURL, Source: runSource, Requested: *total, Workers: *workers, LagNote: "Consumer lag не измеряется генератором; зафиксируйте его отдельно командой scripts/consumer-lag.sh"}
	reportValue.Unique, reportValue.Duplicates, reportValue.Conflicts = uniqueCount, duplicateCount, conflictCount
	client := &http.Client{Timeout: 30 * time.Second}
	// Сначала фиксируем базовые события, затем их повторы: конфликт не может случайно выиграть гонку у оригинала.
	results := make([]outcome, 0, *total)
	results = append(results, executePhase(client, baseURL, createdEvents, *workers)...)
	results = append(results, executePhase(client, baseURL, followupEvents, *workers)...)
	results = append(results, executePhase(client, baseURL, replayEvents, *workers)...)
	latencies := make([]float64, 0, len(results))
	for _, result := range results {
		if result.latency > 0 {
			latencies = append(latencies, float64(result.latency.Microseconds())/1000)
		}
		switch result.status {
		case http.StatusAccepted:
			reportValue.Accepted++
		case http.StatusOK:
			reportValue.DuplicateReplies++
		case http.StatusConflict:
			reportValue.ConflictReplies++
		default:
			reportValue.Errors++
		}
	}
	finished := time.Now().UTC()
	duration := finished.Sub(started)
	reportValue.FinishedAt = finished
	reportValue.DurationMillis = duration.Milliseconds()
	if duration > 0 {
		reportValue.RequestsPerSecond = float64(*total) / duration.Seconds()
	}
	if len(latencies) > 0 {
		sort.Float64s(latencies)
		var sum float64
		for _, value := range latencies {
			sum += value
		}
		reportValue.MeanLatencyMillis = sum / float64(len(latencies))
		reportValue.P95LatencyMillis = percentile(latencies, 0.95)
		reportValue.P99LatencyMillis = percentile(latencies, 0.99)
	}
	encoded, err := json.MarshalIndent(reportValue, "", "  ")
	if err != nil {
		return fmt.Errorf("формирование отчёта: %w", err)
	}
	if *output == "-" {
		fmt.Println(string(encoded))
	} else {
		if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
			return fmt.Errorf("создание каталога отчёта: %w", err)
		}
		if err := os.WriteFile(*output, append(encoded, '\n'), 0o644); err != nil {
			return fmt.Errorf("запись отчёта: %w", err)
		}
		fmt.Printf("Отчёт сохранён: %s\n", *output)
	}
	fmt.Printf("Запросов: %d, принято: %d, дубликатов: %d, конфликтов: %d, ошибок: %d, длительность: %s\n", *total, reportValue.Accepted, reportValue.DuplicateReplies, reportValue.ConflictReplies, reportValue.Errors, duration.Round(time.Millisecond))
	return nil
}

func makeRequests(total, companyCount, paymentCount int, duplicateRate, conflictRate float64, source string, now time.Time) ([]domain.PaymentEvent, []domain.PaymentEvent, []domain.PaymentEvent, int, int, int, error) {
	duplicates := int(float64(total) * duplicateRate)
	conflicts := int(float64(total) * conflictRate)
	unique := total - duplicates - conflicts
	if unique < 1 {
		return nil, nil, nil, 0, 0, 0, errors.New("должен остаться минимум один уникальный запрос")
	}
	base := make([]domain.PaymentEvent, unique)
	createdCount := min(paymentCount, unique)
	runPrefix := source
	for index := range base {
		eventType := domain.EventPaymentCreated
		paymentNumber := index
		if index >= createdCount {
			eventType = domain.EventPaymentSucceeded
			paymentNumber = (index - createdCount) % createdCount
		}
		base[index] = domain.PaymentEvent{
			EventID: fmt.Sprintf("%s-event-%06d", runPrefix, index), Source: source,
			CompanyID: fmt.Sprintf("%s-company-%04d", runPrefix, paymentNumber%companyCount), PaymentID: fmt.Sprintf("%s-payment-%06d", runPrefix, paymentNumber),
			EventType: eventType, AmountMinor: int64(1000 + paymentNumber%100000), Currency: "RUB",
			OccurredAt: now.Add(time.Duration(index) * time.Microsecond), SchemaVersion: domain.SchemaVersion,
		}
	}
	random := rand.New(rand.NewSource(now.UnixNano()))
	replays := make([]domain.PaymentEvent, 0, duplicates+conflicts)
	for index := 0; index < duplicates; index++ {
		replays = append(replays, base[random.Intn(len(base))])
	}
	for index := 0; index < conflicts; index++ {
		event := base[random.Intn(len(base))]
		event.AmountMinor++
		replays = append(replays, event)
	}
	created := append([]domain.PaymentEvent(nil), base[:createdCount]...)
	followups := append([]domain.PaymentEvent(nil), base[createdCount:]...)
	random.Shuffle(len(replays), func(i, j int) { replays[i], replays[j] = replays[j], replays[i] })
	return created, followups, replays, unique, duplicates, conflicts, nil
}

func executePhase(client *http.Client, endpoint string, events []domain.PaymentEvent, workerCount int) []outcome {
	results := make(chan outcome, len(events))
	queue := make(chan domain.PaymentEvent)
	var group sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for event := range queue {
				body, err := event.Marshal()
				if err != nil {
					results <- outcome{status: 0}
					continue
				}
				request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
				if err != nil {
					results <- outcome{status: 0}
					continue
				}
				request.Header.Set("Content-Type", "application/json")
				before := time.Now()
				response, err := client.Do(request)
				latency := time.Since(before)
				if err != nil {
					results <- outcome{status: 0, latency: latency}
					continue
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				results <- outcome{status: response.StatusCode, latency: latency}
			}
		}()
	}
	for _, event := range events {
		queue <- event
	}
	close(queue)
	group.Wait()
	close(results)
	output := make([]outcome, 0, len(events))
	for result := range results {
		output = append(output, result)
	}
	return output
}

func percentile(sortedValues []float64, percentile float64) float64 {
	if len(sortedValues) == 0 {
		return 0
	}
	index := int(float64(len(sortedValues)-1) * percentile)
	return sortedValues[index]
}
