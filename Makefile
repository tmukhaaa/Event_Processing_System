.PHONY: compose-up compose-down migrate test test-integration test-integration-compose e2e load-1k load-10k load-100k load-scenarios

COMPOSE = docker compose -f deploy/docker-compose.yml

compose-up:
	$(COMPOSE) up --build -d

compose-down:
	$(COMPOSE) down

migrate:
	go run ./cmd/migrate

test:
	go test ./...

test-integration:
	go test -count=1 ./internal/repository -run 'Test(Submit|Consumer|Different|Failed|Outbox)'

test-integration-compose:
	docker compose -f deploy/docker-compose.yml run --rm integration-tests

e2e:
	sh tests/e2e.sh

load-1k:
	go run ./cmd/loadgen -events 1000 -output results/loadgen-1k.json

load-10k:
	go run ./cmd/loadgen -events 10000 -output results/loadgen-10k.json

load-100k:
	go run ./cmd/loadgen -events 100000 -output results/loadgen-100k.json

load-scenarios:
	sh scripts/load-scenarios.sh
