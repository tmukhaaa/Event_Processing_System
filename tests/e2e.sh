#!/bin/sh
set -eu

compose='docker compose -f deploy/docker-compose.yml'
suffix=$(date -u +%Y%m%d%H%M%S)
event_id="e2e-event-${suffix}"
payment_id="e2e-payment-${suffix}"
company_id="e2e-company-${suffix}"
occurred_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
event_json="{\"event_id\":\"${event_id}\",\"source\":\"e2e\",\"company_id\":\"${company_id}\",\"payment_id\":\"${payment_id}\",\"event_type\":\"payment.created\",\"amount_minor\":12500,\"currency\":\"RUB\",\"occurred_at\":\"${occurred_at}\",\"schema_version\":1}"

http_status() {
  curl -sS -o /dev/null -w '%{http_code}' http://localhost:8080/api/v1/payment-events \
    -H 'Content-Type: application/json' -d "$1"
}

status=$(http_status "$event_json")
[ "$status" = 202 ] || { printf 'Новое событие вернуло HTTP %s вместо 202\n' "$status" >&2; exit 1; }
status=$(http_status "$event_json")
[ "$status" = 200 ] || { printf 'Дубликат вернул HTTP %s вместо 200\n' "$status" >&2; exit 1; }
conflict_json="{\"event_id\":\"${event_id}\",\"source\":\"e2e\",\"company_id\":\"${company_id}\",\"payment_id\":\"${payment_id}\",\"event_type\":\"payment.created\",\"amount_minor\":12501,\"currency\":\"RUB\",\"occurred_at\":\"${occurred_at}\",\"schema_version\":1}"
status=$(http_status "$conflict_json")
[ "$status" = 409 ] || { printf 'Конфликт вернул HTTP %s вместо 409\n' "$status" >&2; exit 1; }

attempt=0
until curl -sS "http://localhost:8080/api/v1/payments/${payment_id}" | grep -q '"status":"created"'; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    printf 'Платёж не обработан consumer за 60 секунд\n' >&2
    exit 1
  fi
  sleep 1
done

$compose restart consumer >/dev/null
printf '%s|%s\n' "$payment_id" "$event_json" | $compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic payment-events --property parse.key=true --property key.separator='|'

attempt=0
while :; do
  history_count=$($compose exec -T postgres psql -U payment -d payment -Atc \
    "SELECT count(*) FROM payment_history WHERE source='e2e' AND event_id='${event_id}'")
  if [ "$history_count" = 1 ]; then
    break
  fi
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    printf 'Повторная Kafka-доставка создала неверное число записей истории: %s\n' "$history_count" >&2
    exit 1
  fi
  sleep 1
done

metric_events=$($compose exec -T postgres psql -U payment -d payment -Atc \
  "SELECT total_events FROM company_daily_metrics WHERE company_id='${company_id}'")
[ "$metric_events" = 1 ] || { printf 'Агрегат изменён повторно: total_events=%s\n' "$metric_events" >&2; exit 1; }

bad_event_id="e2e-bad-event-${suffix}"
bad_payment_id="e2e-bad-payment-${suffix}"
bad_event_json="{\"event_id\":\"${bad_event_id}\",\"source\":\"e2e\",\"company_id\":\"${company_id}-bad\",\"payment_id\":\"${bad_payment_id}\",\"event_type\":\"payment.succeeded\",\"amount_minor\":12500,\"currency\":\"RUB\",\"occurred_at\":\"${occurred_at}\",\"schema_version\":1}"
status=$(http_status "$bad_event_json")
[ "$status" = 202 ] || { printf 'Синтетическое невалидное для consumer событие вернуло HTTP %s вместо 202\n' "$status" >&2; exit 1; }
attempt=0
while :; do
  dlq_offsets=$($compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka:9092 --topic payment-events-dlq --time -1)
  found=false
  for partition in 0 1 2; do
    latest_offset=$(printf '%s\n' "$dlq_offsets" | awk -F: -v part="$partition" '$2 == part { print $3 }')
    if [ -n "$latest_offset" ] && [ "$latest_offset" -gt 0 ]; then
      message_offset=$((latest_offset - 1))
      dlq_message=$($compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server kafka:9092 --topic payment-events-dlq --partition "$partition" \
        --offset "$message_offset" --max-messages 1 2>/dev/null || true)
      if printf '%s\n' "$dlq_message" | grep -Fq "\"key\":\"${bad_payment_id}\""; then
        found=true
        break
      fi
    fi
  done
  [ "$found" = true ] && break
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 15 ]; then
    printf 'Сообщение после retry не появилось в payment-events-dlq\n' >&2
    exit 1
  fi
  sleep 1
done
printf 'E2E пройден: API dedupe, конфликт, restart/redelivery, analytics dedupe и DLQ.\n'
