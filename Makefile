.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: up
up: ## Build and start the whole stack
	$(COMPOSE) up -d --build
	@echo "API      -> http://localhost:8000/docs"
	@echo "Health   -> http://localhost:8000/health"

.PHONY: up-tools
up-tools: ## Start the stack plus Kafka UI on :8090
	$(COMPOSE) --profile tools up -d --build

.PHONY: down
down: ## Stop the stack (keep volumes)
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop the stack and delete all data
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail api + worker logs
	$(COMPOSE) logs -f api worker

.PHONY: worker-logs
worker-logs: ## Tail worker logs only
	$(COMPOSE) logs -f worker

.PHONY: scale
scale: ## Run 3 worker replicas to show consumer-group rebalancing
	$(COMPOSE) up -d --scale worker=3

.PHONY: demo
demo: ## Run the end-to-end demo against a running stack
	./scripts/demo.sh

.PHONY: install
install: ## Install dependencies locally with uv
	uv sync

.PHONY: test
test: ## Run the unit tests (no infrastructure needed)
	uv run pytest -q

.PHONY: lint
lint: ## Lint and format-check
	uv run ruff check .
	uv run ruff format --check .

.PHONY: fmt
fmt: ## Autoformat
	uv run ruff format .
	uv run ruff check --fix .

.PHONY: topics
topics: ## List Kafka topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

.PHONY: lag
lag: ## Show consumer-group lag for the processor group
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server kafka:9092 --describe --group order-processors

.PHONY: dlq
dlq: ## Dump the dead-letter topic
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server kafka:9092 --topic orders.dlq --from-beginning --timeout-ms 5000

.PHONY: redis-cli
redis-cli: ## Open a redis-cli session
	$(COMPOSE) exec redis redis-cli

.PHONY: keys
keys: ## Show the Redis keyspace
	$(COMPOSE) exec redis redis-cli --scan --count 100
