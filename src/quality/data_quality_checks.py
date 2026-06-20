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
import time
from dataclasses import dataclass
from datetime import UTC

import boto3

logging.basicConfig(level=logging.INFO)  # suppress debug logs to console
logger = logging.getLogger("data_quality")


@dataclass
class CheckResult:
    """Result of a data quality check."""

    check_name: str
    passed: bool
    message: str
    metric_value: float = None
    threshold: float = None
    # "error" blocks the pipeline; "warn" logs but does not fail it.
    # Defaults to "error" so existing checks keep their blocking behavior.
    severity: str = "error"


# ─── Helper Function ────────────────────────────────────────────────────────


def _parse_data_root(data_root: str) -> tuple:
    """Split an s3://bucket/base/prefix URI into (bucket, base_prefix).
    "s3://my-bucket/silver/yellow/" -> ("my-bucket", "silver/yellow/")"""
    no_scheme = data_root.replace("s3://", "").rstrip("/")
    parts = no_scheme.split("/", 1)
    bucket = parts[0]
    base_prefix = (parts[1] + "/") if len(parts) > 1 else ""
    return bucket, base_prefix


# ─── Athena Helper (Tier 2 content checks) ──────────────────────────────────

# Polling tuning. Athena queries are asynchronous: we start one, then poll
# its status until it finishes. Backoff keeps us from hammering the API on
# slow queries, and the timeout stops us looping forever on a stuck query.
_ATHENA_POLL_INITIAL_SEC = 1.0  # first wait between status checks
_ATHENA_POLL_MAX_SEC = 10.0  # cap per-wait so backoff doesn't grow unbounded
_ATHENA_TIMEOUT_SEC = 300.0  # 5 min: generous for a monthly aggregate query


def run_athena_query(
    athena_client,
    query: str,
    database: str,
    workgroup: str,
    output_location: str,
) -> list[dict]:
    """Run an Athena query and return its rows as a list of dicts.

    Athena is asynchronous, so this:
      1. starts the query (start_query_execution),
      2. polls its status with exponential backoff until it finishes,
      3. on success, fetches and parses the result rows.

    Returns a list of {column_name: value} dicts (all values are strings,
    as Athena returns them; callers cast as needed). Raises RuntimeError
    if the query fails, is cancelled, or exceeds the timeout.
    """
    start = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output_location},
    )
    query_id = start["QueryExecutionId"]

    # Poll until the query reaches a terminal state or we time out.
    waited = 0.0
    delay = _ATHENA_POLL_INITIAL_SEC
    while True:
        info = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = info["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = info["QueryExecution"]["Status"].get(
                "StateChangeReason", "no reason given"
            )
            raise RuntimeError(f"Athena query {state}: {reason}")
        if waited >= _ATHENA_TIMEOUT_SEC:
            raise RuntimeError(
                f"Athena query timed out after {_ATHENA_TIMEOUT_SEC:.0f}s "
                f"(last state: {state})"
            )
        time.sleep(delay)
        waited += delay
        delay = min(delay * 2, _ATHENA_POLL_MAX_SEC)  # exponential backoff

    return _parse_athena_results(athena_client, query_id)


def _parse_athena_results(athena_client, query_id: str) -> list[dict]:
    """Turn Athena's get_query_results response into a list of row dicts.

    Athena returns results as a ResultSet whose first row is the column
    headers and remaining rows are data. Each cell is {"VarCharValue": ...}
    (the key is absent for SQL NULLs). This pages through all results.
    """
    rows: list[dict] = []
    column_names: list[str] = []
    next_token = None

    while True:
        kwargs = {"QueryExecutionId": query_id}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = athena_client.get_query_results(**kwargs)

        result_rows = resp["ResultSet"]["Rows"]
        # On the first page, the first row is the header row.
        if not column_names:
            column_names = [
                cell.get("VarCharValue", "") for cell in result_rows[0]["Data"]
            ]
            data_rows = result_rows[1:]
        else:
            data_rows = result_rows

        for row in data_rows:
            # A missing "VarCharValue" key means a SQL NULL -> store None.
            values = [cell.get("VarCharValue") for cell in row["Data"]]
            rows.append(dict(zip(column_names, values, strict=False)))

        next_token = resp.get("NextToken")
        if not next_token:
            break

    return rows


# ─── Check Functions ────────────────────────────────────────────────────────

# list_objects_v2 output dict structure for context
# response = {
#     "KeyCount": 2,
#     "Contents": [
#         {"Key": "silver/part-0.parquet", "Size": 3000, "LastModified": <dt>, ...},
#         {"Key": "silver/part-1.parquet", "Size": 4000, "LastModified": <dt>, ...},
#     ],
# }


def check_s3_object_exists(s3_client, bucket: str, prefix: str) -> CheckResult:
    """Verify that data was actually written to S3."""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    exists = response.get("KeyCount", 0) > 0  # .get() returns zero if Keycount missing
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
    paginator = s3_client.get_paginator("list_objects_v2")  # incase object count > 1000
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


def check_no_unexpected_partitions(
    s3_client, bucket: str, base_prefix: str, expected_year: int
) -> CheckResult:
    """Flag any year=/ partition under a silver/gold path that is not the
    expected year. Catches stray-year partitions from bad source timestamps
    leaking through the transforms (see issue #34)."""
    # List the immediate year=XXXX/ prefixes under the table path.
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=base_prefix, Delimiter="/")
    prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    found_years = []
    for p in prefixes:
        # p looks like ".../year=2024/"
        part = p.rstrip("/").split("/")[-1]
        if part.startswith("year="):
            found_years.append(part.split("=", 1)[1])

    stray = [y for y in found_years if y != str(expected_year)]
    passed = len(stray) == 0
    return CheckResult(
        check_name="no_unexpected_partitions",
        passed=passed,
        message=(
            f"Only expected year={expected_year} present"
            if passed
            else f"STRAY partitions found: {sorted(stray)} (expected only {expected_year})"
        ),
    )


def test_evaluate_results_warn_failure_does_not_block(self):
    """A failed warn-severity check logs but does not fail the suite."""
    from quality.data_quality_checks import CheckResult, evaluate_results

    results = [
        CheckResult("a", True, "ok"),
        CheckResult("b", False, "soft fail", severity="warn"),
    ]
    assert evaluate_results(results) is True


def test_evaluate_results_error_failure_blocks(self):
    """A failed error-severity check fails the suite (default)."""
    from quality.data_quality_checks import CheckResult, evaluate_results

    results = [
        CheckResult("a", True, "ok"),
        CheckResult("b", False, "hard fail"),  # severity defaults to error
    ]
    assert evaluate_results(results) is False


# ─── Composite Check Suites ─────────────────────────────────────────────────


def run_bronze_taxi_checks(
    data_root: str, year: int, month: int, s3_client=None
) -> list[CheckResult]:
    """Run all quality checks for bronze taxi data."""
    s3_client = s3_client or boto3.client("s3")
    bucket, base = _parse_data_root(data_root)
    prefix = f"{base}bronze/nyc_tlc/yellow/year={year}/month={month:02d}/"

    results = [
        check_s3_object_exists(s3_client, bucket, prefix),
        check_s3_file_size(s3_client, bucket, prefix, min_bytes=10_000_000),
        check_s3_file_count(s3_client, bucket, prefix, min_files=1, max_files=5),
    ]
    return results


def run_bronze_weather_checks(
    data_root: str, year: int, month: int, s3_client=None
) -> list[CheckResult]:
    """Run quality checks for bronze weather data (annual NOAA file).

    month is accepted for signature consistency with the other suites but
    is unused — NOAA bronze is a single annual file, not month-partitioned.
    """
    s3_client = s3_client or boto3.client("s3")
    bucket, base = _parse_data_root(data_root)
    prefix = f"{base}bronze/noaa_weather/nyc_daily/year={year}/"

    # Weather files are small (one annual parquet of daily records), so the
    # size/count thresholds are much smaller than taxi.
    results = [
        check_s3_object_exists(s3_client, bucket, prefix),
        check_s3_file_size(s3_client, bucket, prefix, min_bytes=1000),
        check_s3_file_count(s3_client, bucket, prefix, min_files=1, max_files=5),
    ]
    return results


def run_silver_taxi_checks(
    data_root: str, year: int, month: int, s3_client=None
) -> list[CheckResult]:
    """Run all quality checks for silver taxi data."""
    s3_client = s3_client or boto3.client("s3")
    bucket, base = _parse_data_root(data_root)
    prefix = f"{base}silver/nyc_tlc/yellow/year={year}/month={month:02d}/"
    table_base = f"{base}silver/nyc_tlc/yellow/"

    results = [
        check_s3_object_exists(s3_client, bucket, prefix),
        check_s3_file_size(s3_client, bucket, prefix, min_bytes=5_000_000),
        check_s3_file_count(s3_client, bucket, prefix, min_files=1, max_files=200),
        check_no_unexpected_partitions(s3_client, bucket, table_base, year),
    ]
    return results


def run_silver_weather_checks(
    data_root: str, year: int, month: int, s3_client=None
) -> list[CheckResult]:
    """Run quality checks for silver weather data.

    month is accepted for signature consistency but unused — the annual
    NOAA file is processed per-year and partitioned across all 12 months.
    """
    s3_client = s3_client or boto3.client("s3")
    bucket, base = _parse_data_root(data_root)
    prefix = f"{base}silver/noaa_weather/nyc_daily/year={year}/"
    table_base = f"{base}silver/noaa_weather/nyc_daily/"

    results = [
        check_s3_object_exists(s3_client, bucket, prefix),
        check_s3_file_size(s3_client, bucket, prefix, min_bytes=1000),
        check_s3_file_count(s3_client, bucket, prefix, min_files=1, max_files=50),
        check_no_unexpected_partitions(s3_client, bucket, table_base, year),
    ]
    return results


def run_gold_checks(
    data_root: str, year: int, month: int, s3_client=None
) -> list[CheckResult]:
    """Run all quality checks for gold feature tables."""
    s3_client = s3_client or boto3.client("s3")
    bucket, base = _parse_data_root(data_root)

    results = []
    for table in ["trip_weather_daily", "location_hourly_features"]:
        prefix = f"{base}gold/features/{table}/year={year}/month={month:02d}/"
        results.extend(
            [
                check_s3_object_exists(s3_client, bucket, prefix),
                check_s3_file_size(s3_client, bucket, prefix, min_bytes=1000),
            ]
        )

    return results


def evaluate_results(results: list[CheckResult]) -> bool:
    """Log all results and return True if all blocking checks passed.
    A failed check with severity "warn" is logged as a warning but does
    not fail the suite. Only failed "error" checks (the default) block.
    """
    all_passed = True
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        logger.info(f"  [{status}] {r.check_name}: {r.message}")
        if not r.passed:
            if r.severity == "warn":
                logger.warning(
                    f"  [WARN] {r.check_name} failed (non-blocking): {r.message}"
                )
            else:
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
        choices=[
            "bronze_taxi",
            "silver_taxi",
            "bronze_weather",
            "silver_weather",
            "gold",
        ],
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    check_fns = {
        "bronze_taxi": run_bronze_taxi_checks,
        "silver_taxi": run_silver_taxi_checks,
        "bronze_weather": run_bronze_weather_checks,
        "silver_weather": run_silver_weather_checks,
        "gold": run_gold_checks,
    }

    results = check_fns[args.check](args.bucket, args.year, args.month)
    passed = evaluate_results(results)

    if not passed:
        sys.exit(1)
