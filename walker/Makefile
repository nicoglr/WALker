# walker/Makefile
.PHONY: build test lint up down smoke

.ONESHELL:
SHELL := /bin/bash

build:
	go build ./...

test:
	go test ./... -v -count=1

lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run ./...

up:
	docker compose -f ../docker-compose.yml up -d

down:
	docker compose -f ../docker-compose.yml down

smoke: ## End-to-end smoke: bring up stack, run walker, insert a row, check Redis
	@set -euo pipefail
	echo "=== Starting stack ==="
	docker compose -f ../docker-compose.yml up -d postgres redis
	echo "=== Waiting for Postgres ==="
	until docker exec rp_postgres pg_isready -U postgres -d mydb; do sleep 1; done
	echo "=== Building walker ==="
	go build -o /tmp/walker ./cmd/walker
	echo "=== Starting walker in background ==="
	WALKER_PG_DSN="postgres://postgres:postgres@localhost:5432/mydb?replication=database" \
	  WALKER_REDIS_ADDR="localhost:6380" \
	  /tmp/walker &
	WALKER_PID=$$!
	trap 'kill $$WALKER_PID 2>/dev/null || true' EXIT
	sleep 2
	echo "=== Inserting test row ==="
	docker exec rp_postgres psql -U postgres -d mydb -c \
	  "INSERT INTO orders(customer_name, item, quantity, status) VALUES ('Smoke', 'Widget A', 1, 'smoke-test');"
	sleep 1
	echo "=== Checking Redis stream (expect >= 1) ==="
	docker exec rp_redis redis-cli -p 6379 XLEN cdc.mydb.orders
