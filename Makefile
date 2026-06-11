# Root Makefile
.PHONY: up down build test smoke

up:
	docker compose up -d

down:
	docker compose down -v

build:
	$(MAKE) -C walker build

test:
	$(MAKE) -C walker test

smoke:
	$(MAKE) -C walker smoke
