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
import calendar
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


# ─── Check Functions ────────────────────────────────────────────────────────

"""
list_objects_v2 output dict structure for context
response = {
    "KeyCount": 2,
    "Contents": [
        {"Key": "silver/part-0.parquet", "Size": 3000, "LastModified": <dt>, ...},
        {"Key": "silver/part-1.parquet", "Size": 4000, "LastModified": <dt>, ...},
    ],
}
"""


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


# ─── Tier 2: Athena Content Checks ──────────────────────────────────────────

# Row-count floor for silver taxi. NYC yellow does ~3M trips/month; 1M is a
# conservative floor that still catches a badly truncated load.
SILVER_TAXI_MIN_ROWS = 1_000_000

# Trend tolerance: a month within +/-50% of the trailing 3-month average is
# considered normal. Wide because monthly taxi volume genuinely swings with
# season and events; this only catches gross anomalies (a near-empty or
# doubled load), not normal variation.
ROW_COUNT_TREND_TOLERANCE = 0.5


def _athena_count(athena_client, config, database: str, sql: str) -> int:
    """Run a COUNT query and return the integer in its single cell.

    Helper for the count-based checks. Expects sql to select exactly one
    numeric column in one row (e.g. SELECT COUNT(*) AS n ...).
    """
    rows = run_athena_query(
        athena_client,
        sql,
        database=database,
        workgroup=config.ATHENA_WORKGROUP,
        output_location=config.ATHENA_OUTPUT_LOCATION,
    )
    # rows is [{column_name: "value"}]; take the first (only) value.
    first_value = next(iter(rows[0].values()))
    return int(first_value)


def check_row_count_floor(
    athena_client,
    config,
    table: str,
    year: int,
    month: int,
    min_rows: int = SILVER_TAXI_MIN_ROWS,
) -> CheckResult:
    """Fail if the month's row count is below an absolute floor.

    Catches a load that silently truncated (e.g. a partial file or a
    transform that dropped most rows). [error]
    """
    sql = f"SELECT COUNT(*) AS n FROM {table} WHERE year = {year} AND month = {month}"
    count = _athena_count(athena_client, config, config.GLUE_DB_SILVER, sql)
    passed = count >= min_rows
    return CheckResult(
        check_name="row_count_floor",
        passed=passed,
        message=f"Row count {count:,} (floor: {min_rows:,})",
        metric_value=count,
        threshold=min_rows,
        severity="error",
    )


def check_row_count_trend(
    athena_client,
    config,
    table: str,
    year: int,
    month: int,
    tolerance: float = ROW_COUNT_TREND_TOLERANCE,
) -> CheckResult:
    """Warn if the month's row count deviates sharply from recent history.

    Compares this month to the average of the trailing 3 months. If fewer
    than 3 prior months exist, auto-passes (not enough history to judge).
    [warn] — seasonal swings are normal, so this informs rather than blocks.
    """
    # Pull this month and the 3 prior months, counting rows per month.
    # We compute the trailing window in SQL by listing the 4 (year, month)
    # pairs; simpler and cheaper than date math in Athena.
    months = _trailing_months(year, month, n=4)  # includes current month
    pairs = ", ".join(f"({y}, {m})" for (y, m) in months)
    sql = (
        f"SELECT year, month, COUNT(*) AS n FROM {table} "
        f"WHERE (year, month) IN ({pairs}) "
        f"GROUP BY year, month"
    )
    rows = run_athena_query(
        athena_client,
        sql,
        database=config.GLUE_DB_SILVER,
        workgroup=config.ATHENA_WORKGROUP,
        output_location=config.ATHENA_OUTPUT_LOCATION,
    )
    counts = {(int(r["year"]), int(r["month"])): int(r["n"]) for r in rows}

    current = counts.get((year, month), 0)
    prior = [counts.get((y, m), 0) for (y, m) in months if (y, m) != (year, month)]
    prior_present = [c for c in prior if c > 0]

    if len(prior_present) < 3:
        return CheckResult(
            check_name="row_count_trend",
            passed=True,
            message=(
                f"Only {len(prior_present)} prior month(s) of history; "
                "skipping trend check"
            ),
            severity="warn",
        )

    avg = sum(prior_present) / len(prior_present)
    lower = avg * (1 - tolerance)
    upper = avg * (1 + tolerance)
    passed = lower <= current <= upper
    return CheckResult(
        check_name="row_count_trend",
        passed=passed,
        message=(
            f"Row count {current:,} vs trailing avg {avg:,.0f} "
            f"(allowed {lower:,.0f}-{upper:,.0f})"
        ),
        metric_value=current,
        threshold=avg,
        severity="warn",
    )


def _trailing_months(year: int, month: int, n: int) -> list[tuple[int, int]]:
    """Return the n most recent (year, month) pairs ending at (year, month).

    e.g. _trailing_months(2024, 2, 4) -> [(2023,11),(2023,12),(2024,1),(2024,2)]
    Handles year rollover.
    """
    result = []
    y, m = year, month
    for _ in range(n):
        result.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return sorted(result)


# Max acceptable NULL rate per column. 1% allows for rare legitimately-missing
# values without letting a broken column (mostly null) slip through.
MAX_NULL_PCT = 0.01

# Max acceptable range-violation rate per rule. 0.5% tolerates a handful of
# genuine outliers while catching systematic bad data (wrong units, corruption).
MAX_RANGE_VIOLATION_PCT = 0.005


def check_null_rates(
    athena_client,
    config,
    table: str,
    year: int,
    month: int,
    columns: list[str],
    max_null_pct: float = MAX_NULL_PCT,
) -> CheckResult:
    """Fail if any critical column's NULL rate exceeds the threshold.

    Computes NULL percentage per column in a single Athena pass. [error]
    """
    # One null-rate expression per column, all in one query.
    exprs = ", ".join(
        f'SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS "{col}"'
        for col in columns
    )
    sql = f"SELECT {exprs} FROM {table} WHERE year = {year} AND month = {month}"
    rows = run_athena_query(
        athena_client,
        sql,
        database=config.GLUE_DB_SILVER,
        workgroup=config.ATHENA_WORKGROUP,
        output_location=config.ATHENA_OUTPUT_LOCATION,
    )
    row = rows[0]
    violations = {
        col: float(row[col]) for col in columns if float(row[col]) > max_null_pct
    }
    passed = len(violations) == 0
    if passed:
        message = f"All {len(columns)} columns within {max_null_pct:.1%} null rate"
    else:
        detail = ", ".join(f"{c}={p:.2%}" for c, p in violations.items())
        message = f"NULL rate exceeded ({max_null_pct:.1%}): {detail}"
    return CheckResult(
        check_name="null_rates",
        passed=passed,
        message=message,
        severity="error",
    )


def check_value_ranges(
    athena_client,
    config,
    table: str,
    year: int,
    month: int,
    rules: dict,
    max_violation_pct: float = MAX_RANGE_VIOLATION_PCT,
) -> CheckResult:
    """Fail if any column's out-of-range rate exceeds the threshold.

    rules maps column -> (low, high, inclusive_low, inclusive_high). Computes
    the violation percentage per rule in one Athena pass. [error]
    """
    # Build one violation-rate expression per rule.
    exprs = []
    for col, (low, high, incl_low, incl_high) in rules.items():
        lo_op = "<" if incl_low else "<="
        hi_op = ">" if incl_high else ">="
        # A row violates if it's below the low bound or above the high bound.
        exprs.append(
            f"SUM(CASE WHEN {col} {lo_op} {low} OR {col} {hi_op} {high} "
            f'THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS "{col}"'
        )
    sql = (
        f"SELECT {', '.join(exprs)} FROM {table} "
        f"WHERE year = {year} AND month = {month}"
    )
    rows = run_athena_query(
        athena_client,
        sql,
        database=config.GLUE_DB_SILVER,
        workgroup=config.ATHENA_WORKGROUP,
        output_location=config.ATHENA_OUTPUT_LOCATION,
    )
    row = rows[0]
    violations = {
        col: float(row[col]) for col in rules if float(row[col]) > max_violation_pct
    }
    passed = len(violations) == 0
    if passed:
        message = f"All {len(rules)} range rules within {max_violation_pct:.1%}"
    else:
        detail = ", ".join(f"{c}={p:.2%}" for c, p in violations.items())
        message = f"Range violations exceeded ({max_violation_pct:.1%}): {detail}"
    return CheckResult(
        check_name="value_ranges",
        passed=passed,
        message=message,
        severity="error",
    )


def check_dates_in_partition(
    athena_client,
    config,
    table: str,
    year: int,
    month: int,
) -> CheckResult:
    """Fail if any row's pickup_date falls outside the partition's month.

    Catches records whose actual date does not match the year/month
    partition they were written into (bad source timestamps). [error]

    NOTE: assumes Athena partition metadata is in sync with S3. If
    partitions were recently added, run MSCK REPAIR TABLE first.
    """
    # Count rows where pickup_date is outside [first day, last day] of month.
    sql = (
        f"SELECT COUNT(*) AS n FROM {table} "
        f"WHERE year = {year} AND month = {month} "
        f"AND (year(pickup_date) <> {year} OR month(pickup_date) <> {month})"
    )
    count = _athena_count(athena_client, config, config.GLUE_DB_SILVER, sql)
    passed = count == 0
    return CheckResult(
        check_name="dates_in_partition",
        passed=passed,
        message=(
            "All pickup_date values fall within the partition month"
            if passed
            else f"{count:,} rows have pickup_date outside {year}-{month:02d}"
        ),
        metric_value=count,
        threshold=0,
        severity="error",
    )


# Minimum fraction of bronze rows that must survive into silver. Below this,
# the transform is filtering too aggressively (a bug), not just cleaning.
MIN_SILVER_RETENTION = 0.5


def check_calendar_completeness(
    athena_client,
    config,
    database: str,
    table: str,
    year: int,
    month: int,
    date_column: str,
) -> CheckResult:
    """Fail if any calendar day of the month is missing from the table.

    Counts distinct dates present and compares to the number of days in
    the month. Catches gaps (a missing day of weather or gold). [error]
    """
    sql = (
        f"SELECT COUNT(DISTINCT {date_column}) AS n FROM {table} "
        f"WHERE year = {year} AND month = {month}"
    )
    present = _athena_count(athena_client, config, database, sql)
    days_in_month = calendar.monthrange(year, month)[1]
    passed = present == days_in_month
    return CheckResult(
        check_name="calendar_completeness",
        passed=passed,
        message=(
            f"All {days_in_month} days present"
            if passed
            else f"Only {present}/{days_in_month} days present for {year}-{month:02d}"
        ),
        metric_value=present,
        threshold=days_in_month,
        severity="error",
    )


def check_cross_layer_counts(
    athena_client,
    config,
    bronze_table: str,
    silver_table: str,
    year: int,
    month: int,
    min_retention: float = MIN_SILVER_RETENTION,
) -> CheckResult:
    """Fail if silver row count is implausible vs bronze.

    Silver must be <= bronze (cleaning only removes rows) AND >= a minimum
    fraction of bronze (catches over-aggressive filtering). [error]

    NOTE: requires the bronze table to be registered in Glue (issue #50).
    Until then this check cannot run against real data.
    """
    bronze_sql = (
        f"SELECT COUNT(*) AS n FROM {bronze_table} "
        f"WHERE year = {year} AND month = {month}"
    )
    silver_sql = (
        f"SELECT COUNT(*) AS n FROM {silver_table} "
        f"WHERE year = {year} AND month = {month}"
    )
    bronze_count = _athena_count(
        athena_client, config, config.GLUE_DB_BRONZE, bronze_sql
    )
    silver_count = _athena_count(
        athena_client, config, config.GLUE_DB_SILVER, silver_sql
    )

    if bronze_count == 0:
        return CheckResult(
            check_name="cross_layer_counts",
            passed=False,
            message="Bronze count is 0; cannot compare layers",
            severity="error",
        )

    retention = silver_count / bronze_count
    passed = silver_count <= bronze_count and retention >= min_retention
    return CheckResult(
        check_name="cross_layer_counts",
        passed=passed,
        message=(
            f"Silver {silver_count:,} / Bronze {bronze_count:,} "
            f"(retention {retention:.1%}, min {min_retention:.0%})"
        ),
        metric_value=retention,
        threshold=min_retention,
        severity="error",
    )


def check_gold_reconciliation(
    athena_client,
    config,
    year: int,
    month: int,
) -> CheckResult:
    """Fail if gold trip totals don't exactly match silver row count.

    sum(total_trips) in gold trip_weather_daily must equal the silver taxi
    row count for the month. Proves the gold aggregation neither dropped
    nor duplicated trips. [error]
    """
    gold_sql = (
        f"SELECT SUM(total_trips) AS n FROM trip_weather_daily "
        f"WHERE year = {year} AND month = {month}"
    )
    silver_sql = (
        f"SELECT COUNT(*) AS n FROM yellow_taxi_trips "
        f"WHERE year = {year} AND month = {month}"
    )
    gold_total = _athena_count(athena_client, config, config.GLUE_DB_GOLD, gold_sql)
    silver_total = _athena_count(
        athena_client, config, config.GLUE_DB_SILVER, silver_sql
    )
    passed = gold_total == silver_total
    return CheckResult(
        check_name="gold_reconciliation",
        passed=passed,
        message=(
            f"Gold trips {gold_total:,} == silver rows {silver_total:,}"
            if passed
            else f"MISMATCH: gold trips {gold_total:,} vs silver rows "
            f"{silver_total:,} (diff {gold_total - silver_total:+,})"
        ),
        metric_value=gold_total,
        threshold=silver_total,
        severity="error",
    )


def check_stat_bounds(
    athena_client,
    config,
    database: str,
    table: str,
    year: int,
    month: int,
    metric_sql: str,
    lo: float,
    hi: float,
    metric_name: str = "metric",
) -> CheckResult:
    """Warn if a statistical metric falls outside an expected range.

    metric_sql is an aggregate expression (e.g. "AVG(fare_amount)"). This
    is a sanity proxy, not ground truth, so it warns rather than blocks.
    [warn]
    """
    sql = (
        f"SELECT {metric_sql} AS n FROM {table} WHERE year = {year} AND month = {month}"
    )
    rows = run_athena_query(
        athena_client,
        sql,
        database=database,
        workgroup=config.ATHENA_WORKGROUP,
        output_location=config.ATHENA_OUTPUT_LOCATION,
    )
    value = float(next(iter(rows[0].values())))
    passed = lo <= value <= hi
    return CheckResult(
        check_name="stat_bounds",
        passed=passed,
        message=f"{metric_name}={value:.2f} (expected {lo}-{hi})",
        metric_value=value,
        threshold=hi,
        severity="warn",
    )


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
    data_root: str,
    year: int,
    month: int,
    s3_client=None,
    athena_client=None,
    config=None,
) -> list[CheckResult]:
    """Run all quality checks for silver taxi data.

    Tier 1 (cheap S3 metadata) runs first. Only if every Tier 1 check
    passes do we run Tier 2 (Athena content checks), so we never spend
    on Athena when the data isn't even present.
    """
    from config import Config

    s3_client = s3_client or boto3.client("s3")
    config = config or Config()
    bucket, base = _parse_data_root(data_root)
    prefix = f"{base}silver/nyc_tlc/yellow/year={year}/month={month:02d}/"
    table_base = f"{base}silver/nyc_tlc/yellow/"

    # Tier 1: cheap S3 metadata checks.
    results = [
        check_s3_object_exists(s3_client, bucket, prefix),
        check_s3_file_size(s3_client, bucket, prefix, min_bytes=5_000_000),
        check_s3_file_count(s3_client, bucket, prefix, min_files=1, max_files=200),
        check_no_unexpected_partitions(s3_client, bucket, table_base, year),
    ]

    # Gate: skip Athena checks if any Tier 1 (blocking) check failed.
    if not all(r.passed for r in results if r.severity == "error"):
        logger.info("Tier 1 checks failed; skipping Tier 2 Athena checks")
        return results

    athena_client = athena_client or boto3.client("athena")
    table = "yellow_taxi_trips"
    results.extend(
        [
            check_row_count_floor(athena_client, config, table, year, month),
            check_row_count_trend(athena_client, config, table, year, month),
            check_null_rates(
                athena_client,
                config,
                table,
                year,
                month,
                columns=["pickup_datetime", "pickup_location_id", "fare_amount"],
            ),
            check_value_ranges(
                athena_client,
                config,
                table,
                year,
                month,
                rules={
                    # (low, high, inclusive_low, inclusive_high)
                    "fare_amount": (0, 1000, False, True),
                    "trip_distance": (0, 200, False, True),
                    "passenger_count": (1, 8, True, True),
                    "pickup_location_id": (1, 265, True, True),
                },
            ),
            check_dates_in_partition(athena_client, config, table, year, month),
        ]
    )
    return results


def run_silver_weather_checks(
    data_root: str,
    year: int,
    month: int,
    s3_client=None,
    athena_client=None,
    config=None,
) -> list[CheckResult]:
    """Run quality checks for silver weather data.

    Tier 1 first, then Tier 2 (calendar completeness) only if Tier 1
    passed.
    """
    from config import Config

    s3_client = s3_client or boto3.client("s3")
    config = config or Config()
    bucket, base = _parse_data_root(data_root)
    prefix = f"{base}silver/noaa_weather/nyc_daily/year={year}/"
    table_base = f"{base}silver/noaa_weather/nyc_daily/"

    results = [
        check_s3_object_exists(s3_client, bucket, prefix),
        check_s3_file_size(s3_client, bucket, prefix, min_bytes=1000),
        check_s3_file_count(s3_client, bucket, prefix, min_files=1, max_files=50),
        check_no_unexpected_partitions(s3_client, bucket, table_base, year),
    ]

    if not all(r.passed for r in results if r.severity == "error"):
        logger.info("Tier 1 checks failed; skipping Tier 2 Athena checks")
        return results

    athena_client = athena_client or boto3.client("athena")
    results.append(
        check_calendar_completeness(
            athena_client,
            config,
            config.GLUE_DB_SILVER,
            "nyc_weather_daily",
            year,
            month,
            date_column="date",
        )
    )
    return results


def run_gold_checks(
    data_root: str,
    year: int,
    month: int,
    s3_client=None,
    athena_client=None,
    config=None,
) -> list[CheckResult]:
    """Run all quality checks for gold feature tables.

    Tier 1 S3 checks per table first; then Tier 2 (reconciliation,
    completeness, stat bounds) only if Tier 1 passed.
    """
    from config import Config

    s3_client = s3_client or boto3.client("s3")
    config = config or Config()
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

    if not all(r.passed for r in results if r.severity == "error"):
        logger.info("Tier 1 checks failed; skipping Tier 2 Athena checks")
        return results

    athena_client = athena_client or boto3.client("athena")
    results.extend(
        [
            check_gold_reconciliation(athena_client, config, year, month),
            check_calendar_completeness(
                athena_client,
                config,
                config.GLUE_DB_GOLD,
                "trip_weather_daily",
                year,
                month,
                date_column="date",
            ),
            check_stat_bounds(
                athena_client,
                config,
                config.GLUE_DB_GOLD,
                "trip_weather_daily",
                year,
                month,
                "AVG(avg_fare)",
                5,
                50,
                metric_name="avg_fare",
            ),
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
