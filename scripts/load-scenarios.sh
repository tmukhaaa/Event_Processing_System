#!/bin/sh
set -eu

mkdir -p results
stamp=$(date -u +%Y%m%dT%H%M%SZ)
storage_report="results/${stamp}-storage-counts.csv"
printf 'events,duplicate_rate,event_rows,outbox_rows,payment_processed,analytics_processed\n' > "$storage_report"

counts() {
  source=$1
  docker compose -f deploy/docker-compose.yml exec -T postgres psql -U payment -d payment -Atc \
    "SELECT (SELECT count(*) FROM payment_events WHERE source='${source}'), (SELECT count(*) FROM outbox_events WHERE source='${source}'), (SELECT count(*) FROM processed_messages WHERE consumer_name='payment-processor' AND source='${source}'), (SELECT count(*) FROM processed_messages WHERE consumer_name='payment-analytics' AND source='${source}')"
}

for size in ${LOAD_SIZES:-1000 10000 100000}; do
  for rate in 0 0.1 0.3; do
    label=$(printf '%s' "$rate" | tr '.' 'p')
    source="${stamp}-${size}-${label}"
    before=$(counts "$source")
    go run ./cmd/loadgen -events "$size" -payments "$size" -source "$source" -duplicate-rate "$rate" -workers 16 \
      -output "results/${stamp}-${size}-duplicates-${label}.json"
    case "$rate" in
      0) expected=$size ;;
      0.1) expected=$((size - size / 10)) ;;
      0.3) expected=$((size - 3 * size / 10)) ;;
    esac
    before_events=$(printf '%s\n' "$before" | awk -F '|' '{print $1}')
    before_outbox=$(printf '%s\n' "$before" | awk -F '|' '{print $2}')
    before_payment=$(printf '%s\n' "$before" | awk -F '|' '{print $3}')
    before_analytics=$(printf '%s\n' "$before" | awk -F '|' '{print $4}')
    attempt=0
    while :; do
      after=$(counts "$source")
      after_events=$(printf '%s\n' "$after" | awk -F '|' '{print $1}')
      after_outbox=$(printf '%s\n' "$after" | awk -F '|' '{print $2}')
      after_payment=$(printf '%s\n' "$after" | awk -F '|' '{print $3}')
      after_analytics=$(printf '%s\n' "$after" | awk -F '|' '{print $4}')
      if [ "$after_events" -ge "$((before_events + expected))" ] && \
         [ "$after_outbox" -ge "$((before_outbox + expected))" ] && \
         [ "$after_payment" -ge "$((before_payment + expected))" ] && \
         [ "$after_analytics" -ge "$((before_analytics + expected))" ]; then
        break
      fi
      attempt=$((attempt + 1))
      if [ "$attempt" -ge 600 ]; then
        printf 'Consumer не обработал ожидаемые %s событий за 10 минут.\n' "$expected" >&2
        exit 1
      fi
      sleep 1
    done
    printf '%s,%s,%s,%s,%s,%s\n' "$size" "$rate" \
      "$((after_events - before_events))" "$((after_outbox - before_outbox))" \
      "$((after_payment - before_payment))" "$((after_analytics - before_analytics))" >> "$storage_report"
  done
done
printf 'Сценарии завершены. Счётчики строк сохранены: %s. Consumer lag снимите через scripts/consumer-lag.sh.\n' "$storage_report"
