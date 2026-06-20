"""
Tests for data quality checks and Airflow DAG structure.
"""

import os
import sys
from unittest.mock import MagicMock

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
