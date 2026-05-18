"""
Pipeline Configuration
======================
Central config for all pipeline components. Values are loaded from
environment variables (set by Terraform outputs) with sensible defaults.

Usage:
    from src.config import Config
    config = Config()
    print(config.DATA_BUCKET)
"""

import os
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Config:
    """Pipeline configuration loaded from environment variables."""

    # AWS
    AWS_REGION: str = field(
        default_factory=lambda: os.getenv("AWS_REGION", "us-east-1")
    )

    # S3 Buckets
    DATA_BUCKET: str = field(
        default_factory=lambda: os.getenv(
            "DATA_BUCKET", "nyc-taxi-pipeline-data-lake-dev"
        )
    )
    SCRIPTS_BUCKET: str = field(
        default_factory=lambda: os.getenv(
            "SCRIPTS_BUCKET", "nyc-taxi-pipeline-scripts-dev"
        )
    )

    # S3 Prefixes (Medallion Architecture)
    BRONZE_PREFIX: str = "bronze"
    SILVER_PREFIX: str = "silver"
    GOLD_PREFIX: str = "gold"

    # EMR Serverless
    EMR_APP_ID: str = field(default_factory=lambda: os.getenv("EMR_APP_ID", ""))
    EMR_EXECUTION_ROLE_ARN: str = field(
        default_factory=lambda: os.getenv("EMR_EXECUTION_ROLE_ARN", "")
    )

    # Glue Databases
    GLUE_DB_BRONZE: str = field(
        default_factory=lambda: os.getenv(
            "GLUE_DB_BRONZE", "nyc_taxi_pipeline_bronze_dev"
        )
    )
    GLUE_DB_SILVER: str = field(
        default_factory=lambda: os.getenv(
            "GLUE_DB_SILVER", "nyc_taxi_pipeline_silver_dev"
        )
    )
    GLUE_DB_GOLD: str = field(
        default_factory=lambda: os.getenv("GLUE_DB_GOLD", "nyc_taxi_pipeline_gold_dev")
    )

    # Athena
    ATHENA_WORKGROUP: str = field(
        default_factory=lambda: os.getenv("ATHENA_WORKGROUP", "nyc-taxi-pipeline-dev")
    )

    # Data Source URLs
    NYC_TLC_BASE_URL: str = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    NOAA_BASE_URL: str = "https://www.ncei.noaa.gov/data/global-hourly/access"
    # NYC Central Park weather station ID
    NOAA_STATION_ID: str = "72505394728"

    # Pipeline Settings
    # How many months back to look for data on first run (backfill)
    BACKFILL_MONTHS: int = 3
    # NYC TLC data has ~2 month publication lag
    TLC_PUBLICATION_LAG_MONTHS: int = 2

    def s3_path(
        self, layer: str, source: str, dataset: str, year: int = None, month: int = None
    ) -> str:
        """Build a consistent S3 path.

        Example:
            config.s3_path("bronze", "nyc_tlc", "yellow", 2025, 1)
            → "s3://bucket/bronze/nyc_tlc/yellow/year=2025/month=01/"
        """
        path = f"s3://{self.DATA_BUCKET}/{layer}/{source}/{dataset}"
        if year and month:
            path += f"/year={year}/month={month:02d}"
        return path + "/"

    def get_latest_available_month(self) -> tuple[int, int]:
        """Return (year, month) of the most recent TLC data likely available."""
        now = datetime.utcnow()
        # Subtract publication lag
        month = now.month - self.TLC_PUBLICATION_LAG_MONTHS
        year = now.year
        if month <= 0:
            month += 12
            year -= 1
        return year, month
