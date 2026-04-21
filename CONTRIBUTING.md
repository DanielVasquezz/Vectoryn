# Contributing to Vectoryn

Welcome! Vectoryn is a high-scale RAG system built with "Big Tech" engineering standards. To maintain the quality and reliability of the codebase, please follow these guidelines.

## Development Workflow

We use a **Unified Interface** via the `Makefile`. Before submitting any change, ensure it passes all local validations.

### 1. Environment Setup
```bash
cp .env.example .env
# Edit .env and add your GROQ_API_KEY
make dev-infra  # Starts only Kafka, Qdrant, Redis, etc.
```

### 2. Standardized Validations
We use `ruff` for linting, `mypy` for type checking, and `pytest` for testing.
```bash
make ci
```
*Your PR will be automatically blocked by GitHub Actions if `make ci` fails.*

##  Engineering Pillars

Any contribution should adhere to our 6 core pillars:
1. **Foundation**: No hardcoded values. Use environment variables.
2. **Observability**: New features must include logging and instrumentation.
3. **Reliability**: Every new logic must have corresponding unit/integration tests with mocks.
4. **Performance**: Large documents must be chunked using `SemanticChunker`.
5. **Resilience**: Every service must be idempotent and handle failures gracefully.
6. **Efficiency**: Use the `SemanticCache` for repetitive queries.

##  Testing Policy
- **Unit Tests**: No network calls. Use `mock`.
- **Integration Tests**: Test the API surface and contract validation.
- **E2E**: Document how to run them with `TestContainers` if applicable.

##  Docker Best Practices
- Use multi-stage builds.
- Run as a non-root user.
- Keep images slim.

---
*Built for High-Scale AI Infrastructure.*
