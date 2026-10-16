ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
DBT_DIR := $(ROOT_DIR)/dbt
COMPOSE_FILE := $(ROOT_DIR)/infrastructure/docker-compose.yaml
COMPOSE := docker compose --env-file "$(ROOT_DIR)/.env" -f "$(COMPOSE_FILE)"
PYTHON ?= python3
DBT ?= dbt

.PHONY: up down logs ps ingest-all dbt-seed dbt-run dbt-test dbt-docs airflow-up airflow-down

define run_dbt
	cd "$(DBT_DIR)" && $(PYTHON) -c 'import os, sys; from dotenv import load_dotenv; load_dotenv("$(ROOT_DIR)/.env"); os.environ["DBT_PROFILES_DIR"] = "$(DBT_DIR)"; os.execvp("$(DBT)", ["$(DBT)"] + sys.argv[1:])' $(1)
endef

up: airflow-up

down: airflow-down

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

ingest-all:
	cd "$(ROOT_DIR)" && $(PYTHON) ingestion/world_bank.py
	cd "$(ROOT_DIR)" && $(PYTHON) ingestion/imf.py
	cd "$(ROOT_DIR)" && $(PYTHON) ingestion/fx_rates.py

dbt-seed:
	$(call run_dbt,"seed")

dbt-run: dbt-seed
	$(call run_dbt,"run")

dbt-test:
	$(call run_dbt,"test")

dbt-docs:
	$(call run_dbt,docs generate)
	$(call run_dbt,docs serve --host 127.0.0.1 --port 8081)

airflow-up:
	$(COMPOSE) up -d

airflow-down:
	$(COMPOSE) down
