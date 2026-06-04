"""
Pipeline Configuration
======================
Central config for all pipeline components. Values are loaded from
environment variables (set by Terraform outputs) with sensible defaults.

Usage:
    from src.config import Config
    config = Config()
    print(config.DATA_LAKE_ROOT)
"""

import os
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Config:
    """Pipeline configuration loaded from environment variables."""

    # AWS
    AWS_REGION: str = field(
        default_factory=lambda: os.getenv("AWS_REGION", "us-east-2")
    )

    # S3 Locations (full URIs into the shared master bucket)
    DATA_LAKE_ROOT: str = field(
        default_factory=lambda: os.getenv(
            "DATA_LAKE_ROOT", "s3://nateeatsrice-master-s3/data-lake"
        )
    )
    SCRIPTS_LOCATION: str = field(
        default_factory=lambda: os.getenv(
            "SCRIPTS_LOCATION", "s3://nateeatsrice-master-s3/scripts/data-pipeline"
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
        default_factory=lambda: os.getenv("GLUE_DB_BRONZE", "data_pipeline_bronze_dev")
    )
    GLUE_DB_SILVER: str = field(
        default_factory=lambda: os.getenv("GLUE_DB_SILVER", "data_pipeline_silver_dev")
    )
    GLUE_DB_GOLD: str = field(
        default_factory=lambda: os.getenv("GLUE_DB_GOLD", "data_pipeline_gold_dev")
    )

    # Athena
    ATHENA_WORKGROUP: str = field(
        default_factory=lambda: os.getenv("ATHENA_WORKGROUP", "data-pipeline-dev")
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
        path = f"{self.DATA_LAKE_ROOT}/{layer}/{source}/{dataset}"
        if year and month:
            path += f"/year={year}/month={month:02d}"
        return path + "/"

    @property
    def data_bucket_name(self) -> str:
        """Bare bucket name parsed from DATA_LAKE_ROOT (for boto3 calls)."""
        return self.DATA_LAKE_ROOT.replace("s3://", "").split("/", 1)[0]

    @property
    def data_key_prefix(self) -> str:
        """Key prefix under the bucket, e.g. 'data-lake' (no trailing slash)."""
        no_scheme = self.DATA_LAKE_ROOT.replace("s3://", "").rstrip("/")
        parts = no_scheme.split("/", 1)
        return parts[1] if len(parts) > 1 else ""

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
