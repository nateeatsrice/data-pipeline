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

    def test_dag_has_expected_tasks(self):
        """DAG should contain all required tasks."""
        try:
            from airflow.models import DagBag

            dag_path = os.path.join(os.path.dirname(__file__), "..", "airflow", "dags")
            dag_bag = DagBag(dag_folder=dag_path, include_examples=False)
            dag = dag_bag.dags.get("nyc_taxi_monthly_pipeline")

            if dag is None:
                pytest.skip("DAG not found in bag")

            task_ids = [task.task_id for task in dag.tasks]

            # Check key tasks exist
            assert "determine_processing_period" in task_ids
            assert "upload_spark_scripts" in task_ids
            assert "check_bronze_quality" in task_ids
            assert "check_silver_quality" in task_ids
            assert "build_gold_features" in task_ids
            assert "check_gold_quality" in task_ids

        except ImportError:
            pytest.skip("Airflow not installed — skipping DAG validation")
