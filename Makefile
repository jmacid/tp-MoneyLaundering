SHELL := /bin/bash
PWD := $(shell pwd)

up:
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down -v
	find output -mindepth 1 -delete
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

switch:
	@echo Escenarios de prueba:
	@echo "1) Regla 1 - Detección de transacciones menores a 50 USD"
	@echo "2) Regla 2 - Detección de máxima transacción por banco"
	@echo "3) Regla 3 - Detección de transacción menores al promedio cálculado"
	@echo "4) Regla 4 - Detección de patrón Scatter-Gather"
	@echo "5) Regla 5 - Cantidad de transacciones por vía de pago"
	@echo "6) Todas las reglas"
	@read -p "Selecciona uno [1-6]: " option;	\
	cp ./scenarios/$${option}.yaml docker-compose.yaml
.PHONY: switch