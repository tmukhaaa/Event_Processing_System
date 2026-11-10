#!/bin/sh
set -eu

mkdir -p results
stamp=$(date -u +%Y%m%dT%H%M%SZ)
output="results/${stamp}-consumer-lag.txt"
{
  printf 'consumer_group=payment-processor\n'
  docker compose -f deploy/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group payment-processor
  printf '\nconsumer_group=payment-analytics\n'
  docker compose -f deploy/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group payment-analytics
} > "$output"
printf 'Consumer lag сохранён: %s\n' "$output"
