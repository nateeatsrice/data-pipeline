# ============================================================================
# NYC Taxi Pipeline — Common Commands
# ============================================================================
# Uses uv for environment management.
# Usage: make <command>
# Run `make help` to see all available commands.
# ============================================================================

.PHONY: help setup lock test lint terraform-init terraform-plan terraform-apply airflow-up airflow-down deploy-scripts

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─── Setup ─────────────────────────────────────────────────────────────────
setup: ## Install all dependencies (production + dev)
	uv sync --group dev

setup-spark: ## Install with PySpark for local testing
	uv sync --group dev --extra spark

# ─── Lockfile Management ──────────────────────────────────────────────────
lock: ## Resolve dependencies and export pinned requirements for Docker
	uv lock
	uv export --no-dev --format requirements-txt > requirements.lock
	@echo "✅ uv.lock and requirements.lock updated"
	@echo "   requirements.lock is used by the Airflow Dockerfile"

check-lock: ## Verify lockfiles are up to date
	uv lock --check
	@uv export --no-dev --format requirements-txt | diff requirements.lock - \
		&& echo "✅ requirements.lock is in sync" \
		|| (echo "❌ requirements.lock is stale. Run: make lock" && exit 1)

# ─── Testing ───────────────────────────────────────────────────────────────
test: ## Run all unit tests
	uv run pytest tests/ -v --tb=short

test-ingestion: ## Run ingestion tests only
	uv run pytest tests/test_ingestion.py -v

test-spark: ## Run PySpark transformation tests (requires Java + make setup-spark)
	uv run pytest tests/test_transformations.py -v

test-quality: ## Run data quality and DAG tests
	uv run pytest tests/test_quality_and_dag.py -v

test-cov: ## Run tests with coverage report
	uv run pytest tests/ -v --cov=src --cov-report=html --cov-report=term

# ─── Linting ───────────────────────────────────────────────────────────────
lint: ## Run linter
	uv run ruff check src/ tests/

lint-fix: ## Auto-fix linting issues
	uv run ruff check --fix src/ tests/

format: ## Format code
	uv run ruff format src/ tests/

# ─── Terraform ─────────────────────────────────────────────────────────────
terraform-init: ## Initialize Terraform
	cd terraform && terraform init

terraform-plan: ## Preview Terraform changes
	cd terraform && terraform plan

terraform-apply: ## Apply Terraform changes (creates AWS resources)
	cd terraform && terraform apply

terraform-destroy: ## Destroy all Terraform resources
	cd terraform && terraform destroy

terraform-output: ## Show Terraform outputs (for .env file)
	cd terraform && terraform output -json

# ─── Airflow ───────────────────────────────────────────────────────────────
airflow-up: lock ## Start local Airflow (rebuilds if requirements.lock changed)
	cd airflow && docker compose up -d --build

airflow-down: ## Stop local Airflow
	cd airflow && docker compose down

airflow-logs: ## Tail Airflow scheduler logs
	cd airflow && docker compose logs -f airflow-scheduler

airflow-restart: ## Restart Airflow
	cd airflow && docker compose restart

# ─── Deployment ────────────────────────────────────────────────────────────
deploy-scripts: ## Upload PySpark scripts to S3
	@echo "Uploading PySpark scripts to S3..."
	aws s3 sync src/transformation/ $${SCRIPTS_LOCATION}/ \
		--exclude "__pycache__/*" --exclude "*.pyc" --exclude "__init__.py"
	@echo "Done!"

# ─── Manual Ingestion ─────────────────────────────────────────────────────
ingest-latest: ## Manually ingest the latest available taxi data
	uv run python -m src.ingestion.nyc_tlc_ingestion --latest --include-green
