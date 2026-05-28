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

switch:
	@echo Escenarios de prueba:
	@echo "1) Regla 1"
	@echo "2) Regla 2"
	@echo "3) Regla 3"
	@echo "4) Regla 4"
	@echo "5) Regla 5"
	@echo "6) Todas las reglas"
	@read -p "Selecciona uno [1-6]: " option;	\
	cp ./scenarios/$${option}.yaml docker-compose.yaml
.PHONY: switch