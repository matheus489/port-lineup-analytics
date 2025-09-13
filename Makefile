# Ship Lineup Data Pipeline Makefile

.PHONY: help install test clean run-daily run-incremental run-scheduler run-test

help: ## Show this help message
	@echo "Ship Lineup Data Pipeline - Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install -r requirements.txt

setup: ## Initial setup (create directories, config files)
	python setup.py

test: ## Run system test
	python main.py test

run-daily: ## Run daily data collection
	python main.py daily

run-incremental: ## Run incremental data update
	python main.py incremental

run-scheduler: ## Run automated scheduler
	python main.py scheduler

run-manual: ## Run manual collection (requires START_DATE and END_DATE)
	python main.py manual --start-date $(START_DATE) --end-date $(END_DATE)

clean: ## Clean up data and logs
	rm -rf data/bronze/* data/silver/* data/gold/*
	rm -rf logs/*

docker-build: ## Build Docker image
	docker build -t ship-lineup-pipeline .

docker-run: ## Run with Docker Compose
	docker-compose up -d

docker-stop: ## Stop Docker containers
	docker-compose down

docker-logs: ## View Docker logs
	docker-compose logs -f

example: ## Run example usage
	python examples/example_usage.py
