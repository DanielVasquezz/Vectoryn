# ============================================================
# Vectoryn — Makefile
# ============================================================
# LECCIÓN: ¿Para qué sirve un Makefile en 2024?
# ============================================================
# Un Makefile es una interfaz unificada para cualquier desarrollador.
# Sin él, cada dev tiene que recordar comandos distintos:
#   "¿Era docker compose up --build o docker-compose up -d --build?"
#   "¿Cómo se corre el linter? ¿ruff check . o python -m ruff?"
#
# Con Makefile:
#   make dev   → levanta todo (nadie tiene que recordar nada más)
#   make test  → corre todos los tests
#   make lint  → verifica el código
#
# Es convención universal en proyectos open source y enterprise:
# Linux kernel, Kubernetes, React — todos tienen Makefile.
#
# .PHONY = estos targets no son archivos, son comandos.
# Sin .PHONY, Make buscaría un archivo llamado "dev" o "test".
# ============================================================
.PHONY: dev dev-infra dev-build stop logs test lint format clean help

# Variables
COMPOSE = docker compose
PYTHON = python

# ── Colores para output bonito ────────────────────────────────
CYAN  = \033[0;36m
GREEN = \033[0;32m
RESET = \033[0m

## help: Muestra este menú de ayuda
help:
	@echo ""
	@echo "$(CYAN)Vectoryn — Enterprise RAG Pipeline$(RESET)"
	@echo "══════════════════════════════════════"
	@grep -E '^## ' Makefile | sed 's/## /  /'
	@echo ""

## dev: Levanta TODO el sistema (infra + servicios + observabilidad)
dev:
	@echo "$(CYAN) Levantando Vectoryn completo...$(RESET)"
	$(COMPOSE) up -d
	@echo ""
	@echo "$(GREEN)✅ Sistema levantado:$(RESET)"
	@echo "   Ingestion API  →  http://localhost:8000/docs"
	@echo "   Search API     →  http://localhost:8001/docs"
	@echo "   Grafana        →  http://localhost:3000  (admin/vectoryn)"
	@echo "   Prometheus     →  http://localhost:9090"
	@echo "   Jaeger         →  http://localhost:16686"
	@echo "    Qdrant UI      →  http://localhost:6333/dashboard"
	@echo ""

## dev-build: Construye las imágenes y levanta el sistema
dev-build:
	@echo "$(CYAN) Construyendo imágenes Docker...$(RESET)"
	$(COMPOSE) up -d --build

## dev-infra: Solo levanta la infraestructura (Kafka, Qdrant, Redis, Observabilidad)
## Útil para correr los servicios Python localmente (sin Docker)
dev-infra:
	@echo "$(CYAN)  Levantando solo infraestructura...$(RESET)"
	$(COMPOSE) up -d redpanda qdrant redis prometheus grafana jaeger

## stop: Para todos los servicios (preserva los datos en volúmenes)
stop:
	@echo "$(CYAN) Parando todos los servicios...$(RESET)"
	$(COMPOSE) down

## logs: Ver logs en tiempo real de todos los servicios
logs:
	$(COMPOSE) logs -f --tail=100

## logs-ingestion: Ver solo logs del servicio de ingestion
logs-ingestion:
	$(COMPOSE) logs -f ingestion

## logs-search: Ver solo logs del servicio de search
logs-search:
	$(COMPOSE) logs -f search

## logs-worker: Ver solo logs del worker de embeddings
logs-worker:
	$(COMPOSE) logs -f worker

## test: Corre la suite completa de tests
test:
	@echo "$(CYAN) Corriendo tests...$(RESET)"
	$(PYTHON) -m pytest tests/ -v --tb=short --cov=. --cov-report=term-missing

## test-unit: Solo tests unitarios (rápidos, sin Docker)
test-unit:
	$(PYTHON) -m pytest tests/unit/ -v

## test-integration: Tests de integración (requiere servicios corriendo)
test-integration:
	$(PYTHON) -m pytest tests/integration/ -v

## lint: Verifica estilo de código y tipos
lint:
	@echo "$(CYAN) Verificando código...$(RESET)"
	ruff check .
	mypy ingestion/main.py search/api.py --ignore-missing-imports

## format: Formatea automáticamente el código
format:
	@echo "$(CYAN) Formateando código...$(RESET)"
	ruff format .

## ci: Ejecuta toda la suite de validación (Lint + Type Check + Test)
# Es lo mismo que corre el Guardián en GitHub Actions.
ci: format lint test
	@echo "$(GREEN) Todas las validaciones pasaron con éxito.$(RESET)"

## clean-volumes: Borra absolutamente TODO, incluyendo datos indexados
clean-volumes:
	@echo "$(CYAN) Borrando contenedores y VOLÚMENES (Datos)...$(RESET)"
	$(COMPOSE) down -v --remove-orphans

## ingest-test: Envía un documento de prueba a la API de ingestion
ingest-test:
	@echo "$(CYAN) Enviando documento de prueba...$(RESET)"
	curl -s -X POST http://localhost:8000/ingest \
	  -H "Content-Type: application/json" \
	  -H "X-API-Key: your_secret_key_here" \
	  -d '{"content": "Vectoryn es un sistema RAG enterprise con Kafka, Qdrant e Hybrid Search."}' \
	  | python -m json.tool

## search-test: Hace una búsqueda de prueba
search-test:
	@echo "$(CYAN) Probando búsqueda RAG...$(RESET)"
	curl -s -X POST http://localhost:8001/search \
	  -H "Content-Type: application/json" \
	  -H "X-API-Key: your_secret_key_here" \
	  -d '{"query": "¿Qué es Vectoryn?", "top_k": 3}'

