# Makefile for common development tasks

.PHONY: help build up down restart logs smoke db-shell airflow-users profiler

help:
	@echo "Makefile commands:"
	@echo "  make build          - build images (app)"
	@echo "  make up             - start all services in background"
	@echo "  make down           - stop and remove containers"
	@echo "  make restart        - recreate containers (down + up)"
	@echo "  make logs           - tail airflow logs"
	@echo "  make smoke          - run the smoke test script (profiler + checks)"
	@echo "  make db-shell       - open a psql shell to the postgres service"
	@echo "  make airflow-users  - list airflow users inside container"

build:
	docker compose build --pull

up:
	docker compose up -d --build
	docker compose ps

down:
	docker compose down --remove-orphans

restart: down up

logs:
	docker compose logs --no-log-prefix --tail 200 airflow

smoke:
	bash scripts/smoke_check.sh

db-shell:
	docker compose exec -it db psql -U ${POSTGRES_USER} -d ${POSTGRES_DB}

airflow-users:
	docker compose exec -T airflow airflow users list
