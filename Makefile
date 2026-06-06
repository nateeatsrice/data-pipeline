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
	terraform -chdir=terraform/ephemeral init
	terraform -chdir=terraform/persistent init

terraform-plan: ## Preview Terraform changes
	terraform -chdir=terraform/ephemeral plan
	terraform -chdir=terraform/persistent plan

terraform-apply: ## Apply ephemeral stack + regenerate airflow/.env
	terraform -chdir=terraform/ephemeral apply
	./scripts/setup_env.sh
	@echo "Now run: set -a && source airflow/.env && set +a"

terraform-destroy: ## Destroy all ephemeral Terraform resources
	terraform -chdir=terraform/ephemeral destroy

terraform-output: ## Show Terraform outputs (for .env file)
	terraform -chdir=terraform/ephemeral output -json
	terraform -chdir=terraform/persistent output -json

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

# --- Pipeline Runs (EMR Serverless) ---
# Run individual stages or a whole month against EMR Serverless.
# PARAMETER FORMAT (important):
#   YEAR  = full 4 digits, e.g. YEAR=2024   (not 24)
#   MONTH = unpadded 1-12,  e.g. MONTH=3    (not 03; use 3 for March, 12 for Dec)
# These targets source airflow/.env automatically. Taxi ingestion uses ~/ds.
# EMR targets BLOCK: submit, then poll until SUCCESS/FAILED.

_require_year:
	@if [ -z "$(YEAR)" ]; then echo "Missing YEAR. Usage: make <target> YEAR=2024 [MONTH=3]"; exit 1; fi

_require_month:
	@if [ -z "$(MONTH)" ]; then echo "Missing MONTH. Usage: make <target> YEAR=2024 MONTH=3 (unpadded 1-12)"; exit 1; fi

ingest-taxi: _require_year _require_month ## Ingest one month taxi locally via ~/ds. YEAR=2024 MONTH=3
	@echo "Ingesting taxi $(YEAR)-$(MONTH) (local, ~/ds)..."
	~/ds/bin/python -m src.ingestion.nyc_tlc_ingestion --year $(YEAR) --month $(MONTH)

ingest-weather: _require_year ## Ingest one year weather locally via ~/ds. YEAR=2024
	@echo "Ingesting weather $(YEAR) (local, ~/ds)..."
	~/ds/bin/python -m src.ingestion.noaa_weather_ingestion --year $(YEAR)

silver-taxi: _require_year _require_month ## bronze->silver taxi on EMR. YEAR=2024 MONTH=3
	@set -a && . airflow/.env && set +a && \
	JOB_ID=$$(aws emr-serverless start-job-run \
	  --application-id "$$EMR_APP_ID" --execution-role-arn "$$EMR_EXECUTION_ROLE_ARN" \
	  --name "silver-taxi-$(YEAR)-$(MONTH)" \
	  --job-driver '{"sparkSubmit":{"entryPoint":"'"$$SCRIPTS_LOCATION"'/bronze_to_silver_taxi.py","entryPointArguments":["--data-root","'"$$DATA_LAKE_ROOT"'","--year","$(YEAR)","--month","$(MONTH)","--glue-database","'"$$GLUE_DB_SILVER"'"],"sparkSubmitParameters":"--conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.driver.cores=2 --conf spark.driver.memory=4g"}}' \
	  --configuration-overrides '{"monitoringConfiguration":{"s3MonitoringConfiguration":{"logUri":"'"$$DATA_LAKE_ROOT"'/emr-logs/"}}}' \
	  --query 'jobRunId' --output text) && \
	$(MAKE) _poll JOB=$$JOB_ID NAME="silver-taxi-$(YEAR)-$(MONTH)"

silver-weather: _require_year ## bronze->silver weather on EMR. YEAR=2024
	@set -a && . airflow/.env && set +a && \
	JOB_ID=$$(aws emr-serverless start-job-run \
	  --application-id "$$EMR_APP_ID" --execution-role-arn "$$EMR_EXECUTION_ROLE_ARN" \
	  --name "silver-weather-$(YEAR)" \
	  --job-driver '{"sparkSubmit":{"entryPoint":"'"$$SCRIPTS_LOCATION"'/bronze_to_silver_weather.py","entryPointArguments":["--data-root","'"$$DATA_LAKE_ROOT"'","--year","$(YEAR)","--glue-database","'"$$GLUE_DB_SILVER"'"],"sparkSubmitParameters":"--conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.driver.cores=2 --conf spark.driver.memory=4g"}}' \
	  --configuration-overrides '{"monitoringConfiguration":{"s3MonitoringConfiguration":{"logUri":"'"$$DATA_LAKE_ROOT"'/emr-logs/"}}}' \
	  --query 'jobRunId' --output text) && \
	$(MAKE) _poll JOB=$$JOB_ID NAME="silver-weather-$(YEAR)"

gold: _require_year _require_month ## silver->gold features on EMR. YEAR=2024 MONTH=3
	@set -a && . airflow/.env && set +a && \
	JOB_ID=$$(aws emr-serverless start-job-run \
	  --application-id "$$EMR_APP_ID" --execution-role-arn "$$EMR_EXECUTION_ROLE_ARN" \
	  --name "gold-$(YEAR)-$(MONTH)" \
	  --job-driver '{"sparkSubmit":{"entryPoint":"'"$$SCRIPTS_LOCATION"'/silver_to_gold_features.py","entryPointArguments":["--data-root","'"$$DATA_LAKE_ROOT"'","--silver-database","'"$$GLUE_DB_SILVER"'","--gold-database","'"$$GLUE_DB_GOLD"'","--year","$(YEAR)","--month","$(MONTH)"],"sparkSubmitParameters":"--conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.driver.cores=2 --conf spark.driver.memory=4g"}}' \
	  --configuration-overrides '{"monitoringConfiguration":{"s3MonitoringConfiguration":{"logUri":"'"$$DATA_LAKE_ROOT"'/emr-logs/"}}}' \
	  --query 'jobRunId' --output text) && \
	$(MAKE) _poll JOB=$$JOB_ID NAME="gold-$(YEAR)-$(MONTH)"

month: _require_year _require_month ## Full month: silver-taxi then gold. YEAR=2024 MONTH=3
	@$(MAKE) silver-taxi YEAR=$(YEAR) MONTH=$(MONTH)
	@$(MAKE) gold YEAR=$(YEAR) MONTH=$(MONTH)

_poll:  ## checks the status of the EMR run. status can be PENDING, SCHEDULED, RUNNING, SUCCESS, FAILED.
	@set -a && . airflow/.env && set +a && \
	echo "  submitted $(NAME) -> $(JOB)" && \
	while true; do \
	  ST=$$(aws emr-serverless get-job-run --application-id "$$EMR_APP_ID" --job-run-id "$(JOB)" --query 'jobRun.state' --output text); \
	  echo "  $(NAME): $$ST"; \
	  case "$$ST" in \
	    SUCCESS) echo "  done $(NAME)"; break;; \
	    FAILED|CANCELLED) echo "  FAILED $(NAME): $$ST"; aws emr-serverless get-job-run --application-id "$$EMR_APP_ID" --job-run-id "$(JOB)" --query 'jobRun.stateDetails' --output text; exit 1;; \
	    *) sleep 15;; \
	  esac; \
	done

