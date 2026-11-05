SHELL := /bin/bash
PWD := $(shell pwd)

up:
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

PERCENTAGE ?= 10

download:
	docker compose -f docker-compose-download.yaml run --rm -e DOWNLOAD_PERCENTAGE=$(PERCENTAGE) downloader
.PHONY: download

test:
	python3 validation/scenario.py docker-compose.yaml --run --timeout 100000
.PHONY: test

clean-logs:
	make up | python3 purge_logs.py
.PHONY: clean-logs

switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, una sola réplica de cada elemento"
	@echo "2) Múltiples clientes, una sola réplica de cada elemento"
	@echo "3) Un cliente, múltiples réplicas de algunos elementos"
	@echo "4) Múltiples clientes, múltiples réplicas de todos los elementos"
	@read -p "Selecciona uno [1-4]: " option;	\
	cp ./scenarios/$${option}.yaml docker-compose.yaml
.PHONY: switch
