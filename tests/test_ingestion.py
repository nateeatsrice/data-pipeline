"""
Tests for ingestion modules.
Tests the download logic, S3 upload, and idempotency checks.
"""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from ingestion.noaa_weather_ingestion import (
    parse_noaa_precipitation,
    parse_noaa_temperature,
    process_noaa_to_daily,
)
from ingestion.nyc_tlc_ingestion import (
    check_already_ingested,
    check_source_exists,
    ingest_yellow_taxi,
)


class TestNycTlcIngestion:
    """Tests for NYC TLC taxi data ingestion."""

    def test_check_source_exists_returns_true(self):
        """Should return True when source URL responds with 200."""
        with patch("ingestion.nyc_tlc_ingestion.requests.head") as mock_head:
            mock_head.return_value = MagicMock(status_code=200)
            assert check_source_exists("http://example.com/file.parquet") is True

    def test_check_source_exists_returns_false_on_404(self):
        """Should return False when source URL responds with 404."""
        with patch("ingestion.nyc_tlc_ingestion.requests.head") as mock_head:
            mock_head.return_value = MagicMock(status_code=404)
            assert check_source_exists("http://example.com/missing.parquet") is False

    def test_check_source_exists_returns_false_on_exception(self):
        """Should return False when network error occurs."""
        with patch("ingestion.nyc_tlc_ingestion.requests.head") as mock_head:
            mock_head.side_effect = Exception("Connection error")
            assert check_source_exists("http://example.com/file.parquet") is False

    def test_check_already_ingested_returns_false_when_missing(self):
        """Should return False when object doesn't exist in S3."""
        mock_s3 = MagicMock()
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")
        mock_s3.exceptions.ClientError = ClientError
        assert check_already_ingested(mock_s3, "bucket", "key") is False

    def test_check_already_ingested_returns_true_when_exists(self):
        """Should return True when object exists in S3."""
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {"ContentLength": 1000}
        assert check_already_ingested(mock_s3, "bucket", "key") is True

    def test_ingest_skips_when_already_exists(self, test_config):
        """Should skip download when data is already in S3."""
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {"ContentLength": 1000}
        mock_s3.exceptions.ClientError = ClientError

        result = ingest_yellow_taxi(
            2024, 12, config=test_config, s3_client=mock_s3, skip_existing=True
        )

        assert result["success"] is True
        assert result["skipped"] is True

    def test_ingest_fails_gracefully_when_source_unavailable(self, test_config):
        """Should return error when source file doesn't exist yet."""
        mock_s3 = MagicMock()
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")
        mock_s3.exceptions.ClientError = ClientError

        with patch("ingestion.nyc_tlc_ingestion.check_source_exists") as mock_check:
            mock_check.return_value = False
            result = ingest_yellow_taxi(2099, 12, config=test_config, s3_client=mock_s3)

        assert result["success"] is False
        assert result["error"] is not None


class TestNoaaWeatherIngestion:
    """Tests for NOAA weather data ingestion."""

    def test_parse_temperature_valid(self):
        """Should correctly parse NOAA temperature format."""
        import pandas as pd

        series = pd.Series(["+0123,1", "-0050,1", "+9999,9", None])
        result = parse_noaa_temperature(series)
        assert result[0] == pytest.approx(12.3)
        assert result[1] == pytest.approx(-5.0)
        assert pd.isna(result[2])  # Missing value indicator (+9999)
        assert pd.isna(result[3])  # None input

    def test_parse_precipitation_valid(self):
        """Should correctly parse NOAA precipitation format."""
        import pandas as pd

        series = pd.Series(["01,0050,1,1", "01,0000,1,1", None])
        result = parse_noaa_precipitation(series)
        assert result[0] == pytest.approx(5.0)
        assert result[1] == pytest.approx(0.0)
        assert result[2] == pytest.approx(0.0)

    def test_process_noaa_to_daily_aggregation(self):
        """Should aggregate hourly observations to daily summaries."""
        import pandas as pd

        # Create minimal hourly data
        hourly_data = pd.DataFrame(
            {
                "DATE": [
                    "2024-12-15T06:00:00",
                    "2024-12-15T12:00:00",
                    "2024-12-15T18:00:00",
                    "2024-12-16T06:00:00",
                ],
                "TMP": ["+0020,1", "+0050,1", "+0030,1", "+0010,1"],
            }
        )

        daily = process_noaa_to_daily(hourly_data)

        assert len(daily) == 2  # Two days
        assert "temp_avg_celsius" in daily.columns
        assert "observation_count" in daily.columns
        # Dec 15: avg of 2.0, 5.0, 3.0 = 3.33
        dec_15 = daily[daily["date"].astype(str) == "2024-12-15"]
        assert dec_15["observation_count"].values[0] == 3
