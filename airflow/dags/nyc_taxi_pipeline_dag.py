"""
NYC Taxi Pipeline DAG
======================
Monthly pipeline that:
1. Ingests new NYC TLC taxi data + NOAA weather data to bronze
2. Runs quality checks on bronze
3. Transforms bronze → silver via PySpark on EMR Serverless
4. Runs quality checks on silver
5. Builds gold feature tables via PySpark on EMR Serverless
6. Runs quality checks on gold

Schedule: Monthly on the 5th (gives TLC time to publish)
Catchup: True (will backfill missed months)

Architecture Notes:
- Ingestion tasks run as Python callables (lightweight, no Spark needed)
- Transformation tasks submit PySpark jobs to EMR Serverless
- Quality checks run as Python callables after each stage
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessStartJobOperator,
)
from airflow.utils.task_group import TaskGroup
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

# ─── Configuration ───────────────────────────────────────────────────────────
# These come from environment variables set in docker-compose or .env file.
# They map to Terraform outputs.

DATA_BUCKET = os.getenv("DATA_BUCKET", "nyc-taxi-pipeline-data-lake-dev")
SCRIPTS_BUCKET = os.getenv("SCRIPTS_BUCKET", "nyc-taxi-pipeline-scripts-dev")
EMR_APP_ID = os.getenv("EMR_APP_ID", "")
EMR_EXECUTION_ROLE_ARN = os.getenv("EMR_EXECUTION_ROLE_ARN", "")
GLUE_DB_BRONZE = os.getenv("GLUE_DB_BRONZE", "nyc_taxi_pipeline_bronze_dev")
GLUE_DB_SILVER = os.getenv("GLUE_DB_SILVER", "nyc_taxi_pipeline_silver_dev")
GLUE_DB_GOLD = os.getenv("GLUE_DB_GOLD", "nyc_taxi_pipeline_gold_dev")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# TLC publishes data with ~2 month lag, so for a run on 2025-03-05,
# we process data for 2025-01 (the logical_date's month).
TLC_LAG_MONTHS = 2


# ─── Default DAG arguments ──────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,  # Set to True + configure SMTP for alerts
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ─── Helper Functions ────────────────────────────────────────────────────────


def get_processing_period(**context):
    """
    Determine which year/month to process based on the DAG's logical_date.
    Airflow's logical_date is the START of the period, so a monthly DAG
    running on 2025-03-05 has logical_date=2025-03-01. We subtract the
    TLC publication lag to get the data month.
    """
    logical_date = context["logical_date"]
    target = logical_date - relativedelta(months=TLC_LAG_MONTHS)
    year = target.year
    month = target.month
    logger.info(
        f"Processing period: {year}-{month:02d} "
        f"(logical_date={logical_date}, lag={TLC_LAG_MONTHS} months)"
    )
    # Push to XCom so downstream tasks can use it
    context["ti"].xcom_push(key="process_year", value=year)
    context["ti"].xcom_push(key="process_month", value=month)
    return {"year": year, "month": month}


def ingest_taxi_data(**context):
    """Ingest yellow (and optionally green) taxi data."""
    import sys

    sys.path.insert(0, "/opt/airflow/src")
    from config import Config
    from ingestion.nyc_tlc_ingestion import ingest_green_taxi, ingest_yellow_taxi

    ti = context["ti"]
    year = ti.xcom_pull(key="process_year")
    month = ti.xcom_pull(key="process_month")

    config = Config()
    result = ingest_yellow_taxi(year, month, config)
    logger.info(f"Yellow taxi ingestion: {result}")

    if not result["success"] and not result["skipped"]:
        raise Exception(f"Yellow taxi ingestion failed: {result['error']}")

    # Optionally ingest green taxi too
    result_green = ingest_green_taxi(year, month, config)
    logger.info(f"Green taxi ingestion: {result_green}")

    return result


def ingest_weather_data(**context):
    """Ingest NOAA weather data for the processing year."""
    import sys

    sys.path.insert(0, "/opt/airflow/src")
    from config import Config
    from ingestion.noaa_weather_ingestion import ingest_weather

    ti = context["ti"]
    year = ti.xcom_pull(key="process_year")

    config = Config()
    result = ingest_weather(year, config)
    logger.info(f"Weather ingestion: {result}")

    if not result["success"] and not result["skipped"]:
        raise Exception(f"Weather ingestion failed: {result['error']}")

    return result


def run_quality_checks(check_type: str, **context):
    """Run data quality checks for the specified layer."""
    import sys

    sys.path.insert(0, "/opt/airflow/src")
    from quality.data_quality_checks import (
        evaluate_results,
        run_bronze_taxi_checks,
        run_gold_checks,
        run_silver_taxi_checks,
    )

    ti = context["ti"]
    year = ti.xcom_pull(key="process_year")
    month = ti.xcom_pull(key="process_month")

    check_fns = {
        "bronze_taxi": run_bronze_taxi_checks,
        "silver_taxi": run_silver_taxi_checks,
        "gold": run_gold_checks,
    }

    results = check_fns[check_type](DATA_BUCKET, year, month)
    passed = evaluate_results(results)

    if not passed:
        raise Exception(f"Data quality checks FAILED for {check_type}")


def upload_spark_scripts(**context):
    """Upload PySpark scripts to S3 so EMR Serverless can access them."""
    import glob

    import boto3

    s3_client = boto3.client("s3", region_name=AWS_REGION)
    script_dir = "/opt/airflow/src/transformation"

    for filepath in glob.glob(f"{script_dir}/*.py"):
        filename = os.path.basename(filepath)
        s3_key = f"spark-scripts/{filename}"
        logger.info(f"Uploading {filename} to s3://{SCRIPTS_BUCKET}/{s3_key}")
        s3_client.upload_file(filepath, SCRIPTS_BUCKET, s3_key)


# ─── DAG Definition ──────────────────────────────────────────────────────────

with DAG(
    dag_id="nyc_taxi_monthly_pipeline",
    description="Monthly ETL: NYC taxi + weather → bronze → silver → gold",
    default_args=default_args,
    # Run on the 5th of each month at 6:00 AM UTC
    schedule="0 6 5 * *",
    start_date=datetime(2024, 6, 1),
    catchup=True,
    max_active_runs=1,  # Process one month at a time
    tags=["nyc-taxi", "etl", "monthly"],
) as dag:
    # ── Step 0: Determine processing period ──
    determine_period = PythonOperator(
        task_id="determine_processing_period",
        python_callable=get_processing_period,
    )

    # ── Step 1: Upload Spark scripts ──
    upload_scripts = PythonOperator(
        task_id="upload_spark_scripts",
        python_callable=upload_spark_scripts,
    )

    # ── Step 2: Ingest data (parallel) ──
    with TaskGroup("ingestion") as ingestion_group:
        ingest_taxi = PythonOperator(
            task_id="ingest_taxi",
            python_callable=ingest_taxi_data,
        )
        ingest_weather = PythonOperator(
            task_id="ingest_weather",
            python_callable=ingest_weather_data,
        )

    # ── Step 3: Bronze quality checks ──
    check_bronze = PythonOperator(
        task_id="check_bronze_quality",
        python_callable=run_quality_checks,
        op_kwargs={"check_type": "bronze_taxi"},
    )

    # ── Step 4: Bronze → Silver (EMR Serverless) ──
    # These use Airflow's EMR Serverless operators to submit and monitor jobs.
    with TaskGroup("bronze_to_silver") as b2s_group:
        b2s_taxi = EmrServerlessStartJobOperator(
            task_id="transform_taxi",
            application_id=EMR_APP_ID,
            execution_role_arn=EMR_EXECUTION_ROLE_ARN,
            job_driver={
                "sparkSubmit": {
                    "entryPoint": (
                        f"s3://{SCRIPTS_BUCKET}/spark-scripts/bronze_to_silver_taxi.py"
                    ),
                    "entryPointArguments": [
                        "--data-bucket",
                        DATA_BUCKET,
                        "--year",
                        "{{ ti.xcom_pull(key='process_year') }}",
                        "--month",
                        "{{ ti.xcom_pull(key='process_month') }}",
                        "--glue-database",
                        GLUE_DB_SILVER,
                    ],
                    "sparkSubmitParameters": (
                        "--conf spark.executor.cores=2 "
                        "--conf spark.executor.memory=4g "
                        "--conf spark.driver.cores=2 "
                        "--conf spark.driver.memory=4g "
                        "--conf spark.hadoop.hive.metastore.client.factory.class="
                        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    ),
                }
            },
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{DATA_BUCKET}/emr-logs/"
                    }
                }
            },
        )

        b2s_weather = EmrServerlessStartJobOperator(
            task_id="transform_weather",
            application_id=EMR_APP_ID,
            execution_role_arn=EMR_EXECUTION_ROLE_ARN,
            job_driver={
                "sparkSubmit": {
                    "entryPoint": (
                        f"s3://{SCRIPTS_BUCKET}/spark-scripts/"
                        "bronze_to_silver_weather.py"
                    ),
                    "entryPointArguments": [
                        "--data-bucket",
                        DATA_BUCKET,
                        "--year",
                        "{{ ti.xcom_pull(key='process_year') }}",
                        "--glue-database",
                        GLUE_DB_SILVER,
                    ],
                    "sparkSubmitParameters": (
                        "--conf spark.executor.cores=2 "
                        "--conf spark.executor.memory=4g "
                        "--conf spark.driver.cores=2 "
                        "--conf spark.driver.memory=4g "
                        "--conf spark.hadoop.hive.metastore.client.factory.class="
                        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    ),
                }
            },
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{DATA_BUCKET}/emr-logs/"
                    }
                }
            },
        )

    # ── Step 5: Silver quality checks ──
    check_silver = PythonOperator(
        task_id="check_silver_quality",
        python_callable=run_quality_checks,
        op_kwargs={"check_type": "silver_taxi"},
    )

    # ── Step 6: Silver → Gold (EMR Serverless) ──
    s2g_features = EmrServerlessStartJobOperator(
        task_id="build_gold_features",
        application_id=EMR_APP_ID,
        execution_role_arn=EMR_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": (
                    f"s3://{SCRIPTS_BUCKET}/spark-scripts/silver_to_gold_features.py"
                ),
                "entryPointArguments": [
                    "--data-bucket",
                    DATA_BUCKET,
                    "--silver-database",
                    GLUE_DB_SILVER,
                    "--gold-database",
                    GLUE_DB_GOLD,
                    "--year",
                    "{{ ti.xcom_pull(key='process_year') }}",
                    "--month",
                    "{{ ti.xcom_pull(key='process_month') }}",
                ],
                "sparkSubmitParameters": (
                    "--conf spark.executor.cores=2 "
                    "--conf spark.executor.memory=4g "
                    "--conf spark.driver.cores=2 "
                    "--conf spark.driver.memory=4g "
                    "--conf spark.hadoop.hive.metastore.client.factory.class="
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                ),
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": f"s3://{DATA_BUCKET}/emr-logs/"}
            }
        },
    )

    # ── Step 7: Gold quality checks ──
    check_gold = PythonOperator(
        task_id="check_gold_quality",
        python_callable=run_quality_checks,
        op_kwargs={"check_type": "gold"},
    )

    # ── DAG Dependencies ──
    # This defines the execution order (the "directed acyclic graph"):
    #
    # determine_period → upload_scripts → [ingest_taxi, ingest_weather]
    #     → check_bronze → [transform_taxi, transform_weather]
    #     → check_silver → build_gold_features → check_gold

    (
        determine_period
        >> upload_scripts
        >> ingestion_group
        >> check_bronze
        >> b2s_group
        >> check_silver
        >> s2g_features
        >> check_gold
    )
