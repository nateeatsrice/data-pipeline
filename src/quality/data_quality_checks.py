"""
Data Quality Checks
====================
Validates data at each layer of the pipeline.
Called by Airflow after each transformation step.

Checks are designed to FAIL LOUDLY — if data quality drops below
thresholds, the pipeline stops and alerts you rather than silently
propagating bad data downstream.

Usage:
    python -m src.quality.data_quality_checks \
        --check bronze_taxi --bucket my-bucket --year 2024 --month 12
"""

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import UTC

import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_quality")


@dataclass
class CheckResult:
    """Result of a data quality check."""

    check_name: str
    passed: bool
    message: str
    metric_value: float = None
    threshold: float = None


def check_s3_object_exists(s3_client, bucket: str, prefix: str) -> CheckResult:
    """Verify that data was actually written to S3."""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    exists = response.get("KeyCount", 0) > 0
    return CheckResult(
        check_name="s3_object_exists",
        passed=exists,
        message=(
            f"Found objects at s3://{bucket}/{prefix}"
            if exists
            else f"NO objects found at s3://{bucket}/{prefix}"
        ),
    )


def check_s3_file_size(
    s3_client, bucket: str, prefix: str, min_bytes: int = 1000
) -> CheckResult:
    """Verify that files are not suspiciously small (empty/corrupt)."""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:
        return CheckResult(
            check_name="s3_file_size",
            passed=False,
            message=f"No objects found at {prefix}",
        )

    total_size = sum(obj["Size"] for obj in response["Contents"])
    passed = total_size >= min_bytes
    size_mb = total_size / (1024 * 1024)

    return CheckResult(
        check_name="s3_file_size",
        passed=passed,
        message=f"Total size: {size_mb:.2f} MB (min: {min_bytes / 1024:.1f} KB)",
        metric_value=total_size,
        threshold=min_bytes,
    )


def check_s3_file_count(
    s3_client, bucket: str, prefix: str, min_files: int = 1, max_files: int = 1000
) -> CheckResult:
    """Verify reasonable number of output files (catches runaway partitioning)."""
    paginator = s3_client.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        count += page.get("KeyCount", 0)

    passed = min_files <= count <= max_files
    return CheckResult(
        check_name="s3_file_count",
        passed=passed,
        message=f"File count: {count} (expected {min_files}-{max_files})",
        metric_value=count,
    )


def check_s3_freshness(
    s3_client, bucket: str, prefix: str, max_age_hours: int = 48
) -> CheckResult:
    """Verify that data was written recently (catches stale pipelines)."""
    from datetime import datetime

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:
        return CheckResult(
            check_name="s3_freshness",
            passed=False,
            message=f"No objects found at {prefix}",
        )

    latest = max(obj["LastModified"] for obj in response["Contents"])
    age = datetime.now(UTC) - latest
    age_hours = age.total_seconds() / 3600
    passed = age_hours <= max_age_hours

    return CheckResult(
        check_name="s3_freshness",
        passed=passed,
        message=f"Latest file age: {age_hours:.1f} hours (max: {max_age_hours})",
        metric_value=age_hours,
        threshold=max_age_hours,
    )


# ─── Composite Check Suites ─────────────────────────────────────────────────


def run_bronze_taxi_checks(
    bucket: str, year: int, month: int, s3_client=None
) -> list[CheckResult]:
    """Run all quality checks for bronze taxi data."""
    s3_client = s3_client or boto3.client("s3")
    prefix = f"bronze/nyc_tlc/yellow/year={year}/month={month:02d}/"

    results = [
        check_s3_object_exists(s3_client, bucket, prefix),
        check_s3_file_size(s3_client, bucket, prefix, min_bytes=10_000_000),
        check_s3_file_count(s3_client, bucket, prefix, min_files=1, max_files=5),
    ]
    return results


def run_silver_taxi_checks(
    bucket: str, year: int, month: int, s3_client=None
) -> list[CheckResult]:
    """Run all quality checks for silver taxi data."""
    s3_client = s3_client or boto3.client("s3")
    prefix = f"silver/nyc_tlc/yellow/year={year}/month={month:02d}/"

    results = [
        check_s3_object_exists(s3_client, bucket, prefix),
        check_s3_file_size(s3_client, bucket, prefix, min_bytes=5_000_000),
        check_s3_file_count(s3_client, bucket, prefix, min_files=1, max_files=200),
    ]
    return results


def run_gold_checks(
    bucket: str, year: int, month: int, s3_client=None
) -> list[CheckResult]:
    """Run all quality checks for gold feature tables."""
    s3_client = s3_client or boto3.client("s3")

    results = []
    for table in ["trip_weather_daily", "location_hourly_features"]:
        prefix = f"gold/features/{table}/year={year}/month={month:02d}/"
        results.extend(
            [
                check_s3_object_exists(s3_client, bucket, prefix),
                check_s3_file_size(s3_client, bucket, prefix, min_bytes=1000),
            ]
        )

    return results


def evaluate_results(results: list[CheckResult]) -> bool:
    """Log all results and return True if all passed."""
    all_passed = True
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        logger.info(f"  [{status}] {r.check_name}: {r.message}")
        if not r.passed:
            all_passed = False

    if all_passed:
        logger.info("All data quality checks PASSED")
    else:
        logger.error("Some data quality checks FAILED")

    return all_passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        required=True,
        choices=["bronze_taxi", "silver_taxi", "gold"],
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    check_fns = {
        "bronze_taxi": run_bronze_taxi_checks,
        "silver_taxi": run_silver_taxi_checks,
        "gold": run_gold_checks,
    }

    results = check_fns[args.check](args.bucket, args.year, args.month)
    passed = evaluate_results(results)

    if not passed:
        sys.exit(1)
