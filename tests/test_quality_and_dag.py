"""
Tests for data quality checks and Airflow DAG structure.
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from quality.data_quality_checks import (
    CheckResult,
    check_s3_file_size,
    check_s3_object_exists,
    evaluate_results,
)


class TestDataQualityChecks:
    """Tests for individual quality check functions."""

    def test_s3_object_exists_pass(self):
        """Should pass when objects exist at prefix."""
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {"KeyCount": 3}

        result = check_s3_object_exists(mock_s3, "bucket", "bronze/data/")
        assert result.passed is True

    def test_s3_object_exists_fail(self):
        """Should fail when no objects at prefix."""
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {"KeyCount": 0}

        result = check_s3_object_exists(mock_s3, "bucket", "bronze/data/")
        assert result.passed is False

    def test_s3_file_size_pass(self):
        """Should pass when total size exceeds minimum."""
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Size": 5_000_000},
                {"Size": 10_000_000},
            ]
        }

        result = check_s3_file_size(mock_s3, "bucket", "prefix/", min_bytes=1_000_000)
        assert result.passed is True
        assert result.metric_value == 15_000_000

    def test_s3_file_size_fail(self):
        """Should fail when total size is below minimum."""
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {"Contents": [{"Size": 100}]}

        result = check_s3_file_size(mock_s3, "bucket", "prefix/", min_bytes=1_000_000)
        assert result.passed is False

    def test_evaluate_results_all_pass(self):
        """Should return True when all checks pass."""
        results = [
            CheckResult("check1", True, "OK"),
            CheckResult("check2", True, "OK"),
        ]
        assert evaluate_results(results) is True

    def test_evaluate_results_one_fail(self):
        """Should return False when any check fails."""
        results = [
            CheckResult("check1", True, "OK"),
            CheckResult("check2", False, "Data missing"),
        ]
        assert evaluate_results(results) is False

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

    def test_s3_freshness_recent_passes(self):
        """Recent data (within max_age) should pass the freshness check."""
        from datetime import UTC, datetime, timedelta

        from quality.data_quality_checks import check_s3_freshness

        recent = datetime.now(UTC) - timedelta(hours=1)
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "silver/f.parquet", "LastModified": recent}]
        }
        result = check_s3_freshness(mock_s3, "bucket", "silver/", max_age_hours=48)
        assert result.passed is True

    def test_s3_freshness_stale_fails(self):
        """Data older than max_age should fail the freshness check."""
        from datetime import UTC, datetime, timedelta

        from quality.data_quality_checks import check_s3_freshness

        stale = datetime.now(UTC) - timedelta(hours=100)
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "silver/f.parquet", "LastModified": stale}]
        }
        result = check_s3_freshness(mock_s3, "bucket", "silver/", max_age_hours=48)
        assert result.passed is False

    def test_s3_freshness_empty_prefix_fails(self):
        """A prefix with no objects should fail (nothing was written)."""
        from quality.data_quality_checks import check_s3_freshness

        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {}  # no "Contents" key
        result = check_s3_freshness(mock_s3, "bucket", "silver/", max_age_hours=48)
        assert result.passed is False

    def test_no_unexpected_partitions_clean_year_passes(self):
        """Only the expected year present should pass."""
        from quality.data_quality_checks import check_no_unexpected_partitions

        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            "CommonPrefixes": [
                {"Prefix": "silver/yellow/year=2024/"},
            ]
        }
        result = check_no_unexpected_partitions(
            mock_s3, "bucket", "silver/yellow/", expected_year=2024
        )
        assert result.passed is True

    def test_no_unexpected_partitions_stray_year_fails(self):
        """A stray year alongside the expected one should fail."""
        from quality.data_quality_checks import check_no_unexpected_partitions

        # Mirrors the real bug: a stray 2002 partition leaking into the
        # table path next to the expected 2024.
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            "CommonPrefixes": [
                {"Prefix": "silver/yellow/year=2024/"},
                {"Prefix": "silver/yellow/year=2002/"},
            ]
        }
        result = check_no_unexpected_partitions(
            mock_s3, "bucket", "silver/yellow/", expected_year=2024
        )
        assert result.passed is False


class TestParseDataRoot:
    """Tests for the _parse_data_root URI helper."""

    def test_bucket_only(self):
        """A bucket-only URI should yield an empty prefix."""
        from quality.data_quality_checks import _parse_data_root

        assert _parse_data_root("s3://my-bucket") == ("my-bucket", "")

    def test_bucket_with_prefix(self):
        """A bucket+prefix URI should keep the prefix with a trailing slash."""
        from quality.data_quality_checks import _parse_data_root

        bucket, prefix = _parse_data_root("s3://my-bucket/silver/yellow")
        assert bucket == "my-bucket"
        assert prefix == "silver/yellow/"

    def test_trailing_slash_normalized(self):
        """A trailing slash on the input should not double up."""
        from quality.data_quality_checks import _parse_data_root

        assert _parse_data_root("s3://my-bucket/silver/") == (
            "my-bucket",
            "silver/",
        )


class TestAirflowDag:
    """Validate Airflow DAG structure without running it."""

    def test_dag_loads_without_errors(self):
        """DAG file should parse without import errors."""
        # This catches syntax errors and import failures
        dag_path = os.path.join(os.path.dirname(__file__), "..", "airflow", "dags")
        sys.path.insert(0, dag_path)

        try:
            # We need Airflow installed for this test
            from airflow.models import DagBag

            dag_bag = DagBag(dag_folder=dag_path, include_examples=False)
            assert len(dag_bag.import_errors) == 0, (
                f"DAG import errors: {dag_bag.import_errors}"
            )
        except ImportError:
            pytest.skip("Airflow not installed — skipping DAG validation")


class TestAthenaHelper:
    """Tests for the Athena query helper's parsing and state logic."""

    def test_parse_results_maps_headers_and_nulls(self):
        """First row is headers; missing VarCharValue becomes None."""
        from quality.data_quality_checks import _parse_athena_results

        mock_athena = MagicMock()
        mock_athena.get_query_results.return_value = {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "cnt"}, {"VarCharValue": "name"}]},
                    {"Data": [{"VarCharValue": "5"}, {"VarCharValue": "a"}]},
                    {"Data": [{"VarCharValue": "0"}, {}]},  # NULL in 2nd col
                ]
            }
            # no NextToken -> single page
        }
        rows = _parse_athena_results(mock_athena, "qid")
        assert rows == [
            {"cnt": "5", "name": "a"},
            {"cnt": "0", "name": None},
        ]

    def test_query_failure_raises(self):
        """A FAILED query state should raise RuntimeError with the reason."""
        from quality.data_quality_checks import run_athena_query

        mock_athena = MagicMock()
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid"}
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {
                    "State": "FAILED",
                    "StateChangeReason": "SYNTAX_ERROR: bad column",
                }
            }
        }
        with pytest.raises(RuntimeError, match="FAILED"):
            run_athena_query(mock_athena, "SELECT 1", "db", "wg", "s3://out/")


class TestRowCountChecks:
    """Tests for row-count floor and trend checks (tier 2)."""

    def _config(self):
        from config import Config

        return Config()

    def test_row_count_floor_passes_above_floor(self):
        """Count at or above the floor passes."""
        from quality import data_quality_checks as dq

        with patch.object(dq, "run_athena_query", return_value=[{"n": "1500000"}]):
            result = dq.check_row_count_floor(
                MagicMock(), self._config(), "t", 2024, 12, min_rows=1_000_000
            )
        assert result.passed is True
        assert result.metric_value == 1_500_000

    def test_row_count_floor_fails_below_floor(self):
        """Count below the floor fails with error severity."""
        from quality import data_quality_checks as dq

        with patch.object(dq, "run_athena_query", return_value=[{"n": "50000"}]):
            result = dq.check_row_count_floor(
                MagicMock(), self._config(), "t", 2024, 12, min_rows=1_000_000
            )
        assert result.passed is False
        assert result.severity == "error"

    def test_row_count_floor_builds_filtered_sql(self):
        """SQL should COUNT(*) with the year/month filter."""
        from quality import data_quality_checks as dq

        captured = {}

        def fake_query(client, sql, **kwargs):
            captured["sql"] = sql
            return [{"n": "1000000"}]

        with patch.object(dq, "run_athena_query", side_effect=fake_query):
            dq.check_row_count_floor(
                MagicMock(), self._config(), "silver.taxi", 2024, 12
            )
        assert "COUNT(*)" in captured["sql"]
        assert "year = 2024" in captured["sql"]
        assert "month = 12" in captured["sql"]

    def test_row_count_trend_auto_passes_without_history(self):
        """Fewer than 3 prior months -> auto-pass, warn severity."""
        from quality import data_quality_checks as dq

        # Only the current month present, no prior history.
        with patch.object(
            dq,
            "run_athena_query",
            return_value=[{"year": "2024", "month": "1", "n": "9"}],
        ):
            result = dq.check_row_count_trend(MagicMock(), self._config(), "t", 2024, 1)
        assert result.passed is True
        assert result.severity == "warn"

    def test_row_count_trend_fails_outside_tolerance(self):
        """A count far from the trailing average fails (still warn severity)."""
        from quality import data_quality_checks as dq

        # 3 prior months ~1000 each; current month 100 -> way below 50% band.
        rows = [
            {"year": "2023", "month": "10", "n": "1000"},
            {"year": "2023", "month": "11", "n": "1000"},
            {"year": "2023", "month": "12", "n": "1000"},
            {"year": "2024", "month": "1", "n": "100"},
        ]
        with patch.object(dq, "run_athena_query", return_value=rows):
            result = dq.check_row_count_trend(MagicMock(), self._config(), "t", 2024, 1)
        assert result.passed is False
        assert result.severity == "warn"

    def test_trailing_months_handles_year_rollover(self):
        """Trailing window should cross the year boundary correctly."""
        from quality.data_quality_checks import _trailing_months

        assert _trailing_months(2024, 2, 4) == [
            (2023, 11),
            (2023, 12),
            (2024, 1),
            (2024, 2),
        ]


class TestContentChecks:
    """Tests for null-rate, value-range, and date-partition checks."""

    def _config(self):
        from config import Config

        return Config()

    def test_null_rates_pass_under_threshold(self):
        """All columns below the null threshold passes."""
        from quality import data_quality_checks as dq

        # 0.2% and 0.0% null, both under 1%.
        result_row = [{"pickup_datetime": "0.002", "fare_amount": "0.0"}]
        with patch.object(dq, "run_athena_query", return_value=result_row):
            result = dq.check_null_rates(
                MagicMock(),
                self._config(),
                "t",
                2024,
                12,
                columns=["pickup_datetime", "fare_amount"],
            )
        assert result.passed is True

    def test_null_rates_fail_over_threshold(self):
        """A column above the null threshold fails."""
        from quality import data_quality_checks as dq

        # fare_amount 5% null, over 1%.
        result_row = [{"pickup_datetime": "0.0", "fare_amount": "0.05"}]
        with patch.object(dq, "run_athena_query", return_value=result_row):
            result = dq.check_null_rates(
                MagicMock(),
                self._config(),
                "t",
                2024,
                12,
                columns=["pickup_datetime", "fare_amount"],
            )
        assert result.passed is False
        assert "fare_amount" in result.message

    def test_value_ranges_respects_inclusive_bounds(self):
        """Range SQL should use the right operators for inclusive bounds."""
        from quality import data_quality_checks as dq

        captured = {}

        def fake_query(client, sql, **kwargs):
            captured["sql"] = sql
            return [{"passenger_count": "0.0"}]

        rules = {"passenger_count": (1, 8, True, True)}  # [1, 8] inclusive
        with patch.object(dq, "run_athena_query", side_effect=fake_query):
            dq.check_value_ranges(MagicMock(), self._config(), "t", 2024, 12, rules)
        # Inclusive low means violation is "< 1"; inclusive high "> 8".
        assert "passenger_count < 1" in captured["sql"]
        assert "passenger_count > 8" in captured["sql"]

    def test_value_ranges_fail_over_threshold(self):
        """A rule violated above the threshold fails."""
        from quality import data_quality_checks as dq

        # 2% out of range, over 0.5%.
        with patch.object(
            dq, "run_athena_query", return_value=[{"fare_amount": "0.02"}]
        ):
            result = dq.check_value_ranges(
                MagicMock(),
                self._config(),
                "t",
                2024,
                12,
                rules={"fare_amount": (0, 1000, False, True)},
            )
        assert result.passed is False

    def test_dates_in_partition_pass_when_zero(self):
        """Zero out-of-month rows passes."""
        from quality import data_quality_checks as dq

        with patch.object(dq, "run_athena_query", return_value=[{"n": "0"}]):
            result = dq.check_dates_in_partition(
                MagicMock(), self._config(), "t", 2024, 12
            )
        assert result.passed is True

    def test_dates_in_partition_fail_when_nonzero(self):
        """Any out-of-month rows fails."""
        from quality import data_quality_checks as dq

        with patch.object(dq, "run_athena_query", return_value=[{"n": "42"}]):
            result = dq.check_dates_in_partition(
                MagicMock(), self._config(), "t", 2024, 12
            )
        assert result.passed is False
        assert result.metric_value == 42


class TestCompletenessAndCrossLayer:
    """Tests for calendar completeness and cross-layer count checks."""

    def _config(self):
        from config import Config

        return Config()

    def test_calendar_completeness_all_days_present(self):
        """All days present (31 for December) passes."""
        from quality import data_quality_checks as dq

        with patch.object(dq, "run_athena_query", return_value=[{"n": "31"}]):
            result = dq.check_calendar_completeness(
                MagicMock(), self._config(), "db", "t", 2024, 12, "date"
            )
        assert result.passed is True

    def test_calendar_completeness_missing_day_fails(self):
        """A missing day (30 of 31) fails."""
        from quality import data_quality_checks as dq

        with patch.object(dq, "run_athena_query", return_value=[{"n": "30"}]):
            result = dq.check_calendar_completeness(
                MagicMock(), self._config(), "db", "t", 2024, 12, "date"
            )
        assert result.passed is False

    def test_calendar_completeness_handles_february_leap(self):
        """February 2024 (leap year) expects 29 days."""
        from quality import data_quality_checks as dq

        with patch.object(dq, "run_athena_query", return_value=[{"n": "29"}]):
            result = dq.check_calendar_completeness(
                MagicMock(), self._config(), "db", "t", 2024, 2, "date"
            )
        assert result.passed is True

    def test_cross_layer_counts_pass_normal_retention(self):
        """Silver below bronze and above min retention passes."""
        from quality import data_quality_checks as dq

        # bronze 1000, silver 900 -> 90% retention.
        with patch.object(
            dq, "run_athena_query", side_effect=[[{"n": "1000"}], [{"n": "900"}]]
        ):
            result = dq.check_cross_layer_counts(
                MagicMock(), self._config(), "b", "s", 2024, 12
            )
        assert result.passed is True

    def test_cross_layer_counts_fail_over_filtering(self):
        """Silver far below bronze (under min retention) fails."""
        from quality import data_quality_checks as dq

        # bronze 1000, silver 100 -> 10% retention, under 50%.
        with patch.object(
            dq, "run_athena_query", side_effect=[[{"n": "1000"}], [{"n": "100"}]]
        ):
            result = dq.check_cross_layer_counts(
                MagicMock(), self._config(), "b", "s", 2024, 12
            )
        assert result.passed is False

    def test_cross_layer_counts_fail_silver_exceeds_bronze(self):
        """Silver larger than bronze (impossible) fails."""
        from quality import data_quality_checks as dq

        # bronze 1000, silver 1200 -> silver > bronze, invalid.
        with patch.object(
            dq, "run_athena_query", side_effect=[[{"n": "1000"}], [{"n": "1200"}]]
        ):
            result = dq.check_cross_layer_counts(
                MagicMock(), self._config(), "b", "s", 2024, 12
            )
        assert result.passed is False


class TestReconciliationAndStats:
    """Tests for gold reconciliation and statistical-bounds checks."""

    def _config(self):
        from config import Config

        return Config()

    def test_gold_reconciliation_exact_match_passes(self):
        """Gold trip total equal to silver row count passes."""
        from quality import data_quality_checks as dq

        # gold sum 1000, silver count 1000 -> match.
        with patch.object(
            dq, "run_athena_query", side_effect=[[{"n": "1000"}], [{"n": "1000"}]]
        ):
            result = dq.check_gold_reconciliation(MagicMock(), self._config(), 2024, 12)
        assert result.passed is True

    def test_gold_reconciliation_mismatch_fails(self):
        """Any difference between gold and silver totals fails."""
        from quality import data_quality_checks as dq

        # gold 990, silver 1000 -> 10 trips lost in aggregation.
        with patch.object(
            dq, "run_athena_query", side_effect=[[{"n": "990"}], [{"n": "1000"}]]
        ):
            result = dq.check_gold_reconciliation(MagicMock(), self._config(), 2024, 12)
        assert result.passed is False
        assert "MISMATCH" in result.message

    def test_stat_bounds_within_range_passes(self):
        """A metric inside the expected range passes."""
        from quality import data_quality_checks as dq

        # avg fare 18.50, within [5, 50].
        with patch.object(dq, "run_athena_query", return_value=[{"n": "18.5"}]):
            result = dq.check_stat_bounds(
                MagicMock(),
                self._config(),
                "db",
                "t",
                2024,
                12,
                "AVG(fare_amount)",
                5,
                50,
                metric_name="avg_fare",
            )
        assert result.passed is True

    def test_stat_bounds_outside_range_warns(self):
        """A metric outside the range fails but with warn severity."""
        from quality import data_quality_checks as dq

        # avg fare 120, above [5, 50].
        with patch.object(dq, "run_athena_query", return_value=[{"n": "120.0"}]):
            result = dq.check_stat_bounds(
                MagicMock(),
                self._config(),
                "db",
                "t",
                2024,
                12,
                "AVG(fare_amount)",
                5,
                50,
                metric_name="avg_fare",
            )
        assert result.passed is False
        assert result.severity == "warn"


class TestSuiteWiring:
    """Tests for tier-1-first gating in the composite suites."""

    def test_silver_taxi_skips_tier2_when_tier1_fails(self):
        """If a tier 1 check fails, tier 2 Athena checks are skipped."""
        from quality import data_quality_checks as dq

        # Make the first tier 1 check fail; Athena client should never be
        # created/used, so patch run_athena_query to blow up if called.
        failing = dq.CheckResult("s3_object_exists", False, "missing")
        with (
            patch.object(dq, "check_s3_object_exists", return_value=failing),
            patch.object(dq, "run_athena_query", side_effect=AssertionError("called!")),
        ):
            results = dq.run_silver_taxi_checks(
                "s3://b/data-lake", 2024, 12, s3_client=MagicMock()
            )
        # Only tier 1 results present; no tier 2 ran.
        assert any(not r.passed for r in results)
        assert all(
            r.check_name not in ("row_count_floor", "null_rates", "value_ranges")
            for r in results
        )

    def test_silver_weather_runs_completeness_when_tier1_passes(self):
        """When tier 1 passes, the weather suite runs the completeness check."""
        from quality import data_quality_checks as dq

        passing = dq.CheckResult("t1", True, "ok")
        completeness = dq.CheckResult("calendar_completeness", True, "ok")
        with (
            patch.object(dq, "check_s3_object_exists", return_value=passing),
            patch.object(dq, "check_s3_file_size", return_value=passing),
            patch.object(dq, "check_s3_file_count", return_value=passing),
            patch.object(dq, "check_no_unexpected_partitions", return_value=passing),
            patch.object(
                dq, "check_calendar_completeness", return_value=completeness
            ) as mock_cc,
        ):
            dq.run_silver_weather_checks(
                "s3://b/data-lake",
                2024,
                12,
                s3_client=MagicMock(),
                athena_client=MagicMock(),
            )
        assert mock_cc.called
