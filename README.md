# NYC Taxi Data Pipeline

A production-style batch data engineering pipeline that ingests NYC TLC taxi trip data and NOAA weather data, transforms it through a medallion architecture (bronze → silver → gold), and produces feature tables for data science projects.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Airflow (Local Docker)                       │
│                                                                     │
│  ┌───────────┐   ┌──────────┐   ┌──────────┐   ┌──────────────────┐ │
│  │ Determine │──▶│  Ingest  │──▶│  Quality │──▶│ Bronze → Silver  │ │
│  │  Period   │   │  (Python)│   │  Checks  │   │ (EMR Serverless) │ │
│  └───────────┘   └──────────┘   └──────────┘   └───────┬──────────┘ │
│                                                        │            │
│   ┌──────────────────┐   ┌──────────┐   ┌──────────────▼──────────┐ │
│   │  Quality Checks  │◀──│  Gold    │◀──│  Silver → Gold          │ │
│   │  (Gold Layer)    │   │  Checks  │   │  (EMR Serverless)       │ │
│   └──────────────────┘   └──────────┘   └─────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │       AWS S3          │
                    │  ┌───────────────┐    │
                    │  │    Bronze     │    │  ← Raw parquet from sources
                    │  │  (raw data)   │    │
                    │  ├───────────────┤    │
                    │  │    Silver     │    │  ← Cleaned, typed, deduped
                    │  │  (cleaned)    │    │
                    │  ├───────────────┤    │
                    │  │     Gold      │    │  ← Feature tables for ML
                    │  │  (features)   │    │
                    │  └───────────────┘    │
                    └──────────┬────────────┘
                               │
              ┌────────────────┼──────────────────┐
              │                │                  │
     ┌────────▼──────┐  ┌──────▼───────┐  ┌───────▼───────┐
     │  Glue Catalog │  │    Athena    │  │ Data Science  │
     │  (Schema)     │  │   (SQL)      │  │  (Notebooks)  │
     └───────────────┘  └──────────────┘  └───────────────┘
```

## Data Sources

| Source | Update Frequency | Format | Description |
|--------|-----------------|--------|-------------|
| NYC TLC Yellow Taxi | Monthly (~2 month lag) | Parquet | Trip records: pickups, dropoffs, fares, tips |
| NYC TLC Green Taxi  | Monthly (~2 month lag) | Parquet | Outer borough taxi trips |
| NOAA Weather (NYC)  | Daily | CSV → Parquet | Temperature, precipitation, wind for Central Park |

## Gold Feature Tables

### `trip_weather_daily`
Daily taxi aggregates joined with weather. Use for analyzing weather impact on ridership.

Key columns: `total_trips`, `avg_fare`, `avg_tip_percentage`, `total_revenue`, `temp_avg_fahrenheit`, `is_rainy`, `is_weekend`

### `location_hourly_features`
Per-zone, per-hour demand metrics. Use for demand forecasting and surge analysis.

Key columns: `trip_count`, `avg_distance`, `avg_fare`, `unique_destinations`, `time_of_day`, `is_weekend`

## Prerequisites

- **AWS Account** with CLI configured (`aws configure`)
- **Terraform** >= 1.5 
- **Docker** & Docker Compose
- **Python** 3.11+ 
- **uv** 

## Quick Start

### 1. Clone and Install

```bash
git clone https://github.com/your-username/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline

# Install production + dev dependencies (creates .venv automatically)
make setup

# Or with PySpark for local transformation testing
make setup-spark
```

### 2. Deploy Infrastructure

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values

terraform init
terraform plan      # Review what will be created
terraform apply     # Create AWS resources (~30 seconds)
```

### 3. Wire Up Airflow

```bash
# Auto-populate Airflow .env from Terraform outputs
chmod +x scripts/setup_env.sh
./scripts/setup_env.sh

# Upload PySpark scripts to S3
make deploy-scripts
```

### 4. Start Airflow

```bash
cd airflow
docker compose up -d

# Wait ~30 seconds for initialization, then open:
# http://localhost:8080 (username: admin, password: admin)
```

### 5. Run the Pipeline

In the Airflow UI:
1. Toggle the `nyc_taxi_monthly_pipeline` DAG to "on"
2. Click "Trigger DAG" to run immediately, or wait for the monthly schedule

### 6. Query Feature Tables

```sql
-- In Athena (AWS Console → Athena → select your workgroup)
SELECT
    pickup_date,
    total_trips,
    avg_tip_percentage,
    temp_avg_fahrenheit,
    is_rainy
FROM nyc_taxi_pipeline_gold_dev.trip_weather_daily
WHERE year = 2024 AND month = 12
ORDER BY pickup_date;
```

### 7. Use in Python (Data Science)

```python
import boto3
import pandas as pd

# Option 1: Read directly from S3
df = pd.read_parquet(
    "s3://nyc-taxi-pipeline-data-lake-dev/gold/features/trip_weather_daily/"
)

# Option 2: Query via Athena (for filtered reads)
import awswrangler as wr
df = wr.athena.read_sql_query(
    "SELECT * FROM trip_weather_daily WHERE is_rainy = true",
    database="nyc_taxi_pipeline_gold_dev",
    workgroup="nyc-taxi-pipeline-dev",
)
```

## Development

```bash
make help           # See all available commands
make setup          # Install dependencies via uv
make test           # Run all tests
make lint           # Check code quality
make format         # Auto-format code
make test-cov       # Tests with coverage report
make lock           # Re-resolve deps and update requirements.lock
make check-lock     # Verify lockfiles are in sync (runs in CI)
```

## Project Structure

```
nyc-taxi-pipeline/
├── pyproject.toml                 # Dependencies & tool config (single source of truth)
├── uv.lock                        # Pinned dependency lockfile (committed)
├── requirements.lock              # Exported from uv.lock for Airflow Dockerfile
├── terraform/                     # Infrastructure as Code
│   ├── main.tf                    # Provider configuration
│   ├── variables.tf               # Input variables
│   ├── outputs.tf                 # Output values
│   ├── s3.tf                      # S3 buckets (data lake)
│   ├── iam.tf                     # IAM roles (least privilege)
│   ├── emr_serverless.tf          # EMR Serverless app
│   ├── glue.tf                    # Glue Data Catalog
│   └── athena.tf                  # Athena workgroup
├── src/
│   ├── config.py                  # Central configuration
│   ├── ingestion/                 # Data download & upload to bronze
│   │   ├── nyc_tlc_ingestion.py
│   │   └── noaa_weather_ingestion.py
│   ├── transformation/            # PySpark jobs (run on EMR)
│   │   ├── bronze_to_silver_taxi.py
│   │   ├── bronze_to_silver_weather.py
│   │   └── silver_to_gold_features.py
│   └── quality/                   # Data quality validation
│       └── data_quality_checks.py
├── airflow/
│   ├── dags/
│   │   └── nyc_taxi_pipeline_dag.py
│   ├── docker-compose.yaml
│   └── Dockerfile                 # Uses requirements.lock for parity
├── tests/                         # Pytest test suite
├── scripts/                       # Helper scripts
├── .github/workflows/ci.yml       # CI pipeline (uses uv)
└── Makefile                       # Common commands (all use uv)
```

## Adding a New Data Source

The pipeline is designed for extensibility. To add a new source:

1. **Create an ingestion script** in `src/ingestion/new_source_ingestion.py`
2. **Create transformation scripts** in `src/transformation/`
3. **Add tasks to the DAG** (or create a new DAG) in `airflow/dags/`
4. **Add quality checks** in `src/quality/`
5. **Add tests** in `tests/`
6. The S3 structure, Glue catalog, and IAM permissions already support new sources.

## Cost Estimate (Monthly) 

| Service | Usage | Cost |
|---------|-------|------|
| S3 Storage | ~5 GB | $0.12 |
| S3 Requests | ~10,000 | $0.05 |
| EMR Serverless | ~15 min compute/month | $0.50–2.00 |
| Athena | ~1 GB scanned/month | $0.005 |
| Glue Catalog | Free tier | $0.00 |
| **Total** | | **~$1–3/month** |

## Cleanup

```bash
# Destroy all AWS resources
make terraform-destroy

# Stop local Airflow
make airflow-down
```
