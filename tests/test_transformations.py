"""
Tests for PySpark transformation logic.
Uses a local SparkSession (no EMR needed) with sample data.

Note: These tests require pyspark to be installed locally.
Run with: pytest tests/test_transformations.py -v

"""

import os
import sys

import pytest

# Check if PySpark is available.
try:
    from pyspark.sql import SparkSession

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing."""
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark not installed")

    session = (
        SparkSession.builder.master("local[1]")
        .appName("test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_taxi_df(spark, sample_taxi_data):
    """Create a Spark DataFrame from sample taxi data."""
    return spark.createDataFrame(sample_taxi_data)


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not installed")
class TestBronzeToSilverTaxi:
    """Tests for taxi data cleaning transformation."""

    def test_column_renaming(self, spark, sample_taxi_df):
        """Should rename TLC columns to snake_case."""
        from transformation.bronze_to_silver_taxi import clean_yellow_taxi

        result = clean_yellow_taxi(sample_taxi_df, 2024, 12)
        assert "vendor_id" in result.columns
        assert "pickup_datetime" in result.columns
        assert "dropoff_datetime" in result.columns
        assert "pickup_location_id" in result.columns
        assert "VendorID" not in result.columns

    def test_derived_columns_added(self, spark, sample_taxi_df):
        """Should add pickup_date, pickup_hour, trip_duration_minutes."""
        from transformation.bronze_to_silver_taxi import clean_yellow_taxi

        result = clean_yellow_taxi(sample_taxi_df, 2024, 12)
        assert "pickup_date" in result.columns
        assert "pickup_hour" in result.columns
        assert "pickup_day_of_week" in result.columns
        assert "trip_duration_minutes" in result.columns
        assert "taxi_type" in result.columns

    def test_trip_duration_calculation(self, spark, sample_taxi_df):
        """Should correctly calculate trip duration in minutes."""
        from transformation.bronze_to_silver_taxi import clean_yellow_taxi

        result = clean_yellow_taxi(sample_taxi_df, 2024, 12)
        # First trip: 08:30 to 08:45 = 15 minutes
        first_trip = result.orderBy("pickup_datetime").first()
        assert first_trip["trip_duration_minutes"] == pytest.approx(15.0, abs=0.1)

    def test_invalid_records_filtered(self, spark):
        """Should remove records with invalid values."""
        from transformation.bronze_to_silver_taxi import clean_yellow_taxi

        # Create data with one valid and one invalid record
        data = [
            {
                "VendorID": 1,
                "tpep_pickup_datetime": "2024-12-15 08:30:00",
                "tpep_dropoff_datetime": "2024-12-15 08:45:00",
                "passenger_count": 2,
                "trip_distance": 3.5,
                "RatecodeID": 1,
                "store_and_fwd_flag": "N",
                "PULocationID": 161,
                "DOLocationID": 237,
                "payment_type": 1,
                "fare_amount": 15.50,
                "extra": 1.00,
                "mta_tax": 0.50,
                "tip_amount": 3.50,
                "tolls_amount": 0.00,
                "improvement_surcharge": 1.00,
                "total_amount": 21.50,
            },
            {
                # Invalid: negative trip distance
                "VendorID": 1,
                "tpep_pickup_datetime": "2024-12-15 10:00:00",
                "tpep_dropoff_datetime": "2024-12-15 10:15:00",
                "passenger_count": 1,
                "trip_distance": -5.0,
                "RatecodeID": 1,
                "store_and_fwd_flag": "N",
                "PULocationID": 100,
                "DOLocationID": 200,
                "payment_type": 1,
                "fare_amount": 10.00,
                "extra": 0.00,
                "mta_tax": 0.50,
                "tip_amount": 0.00,
                "tolls_amount": 0.00,
                "improvement_surcharge": 1.00,
                "total_amount": 11.50,
            },
        ]

        df = spark.createDataFrame(data)
        result = clean_yellow_taxi(df, 2024, 12)
        assert result.count() == 1  # Only valid record survives

    def test_deduplication(self, spark):
        """Should remove exact duplicate records."""
        from transformation.bronze_to_silver_taxi import clean_yellow_taxi

        # Create two identical records
        record = {
            "VendorID": 1,
            "tpep_pickup_datetime": "2024-12-15 08:30:00",
            "tpep_dropoff_datetime": "2024-12-15 08:45:00",
            "passenger_count": 2,
            "trip_distance": 3.5,
            "RatecodeID": 1,
            "store_and_fwd_flag": "N",
            "PULocationID": 161,
            "DOLocationID": 237,
            "payment_type": 1,
            "fare_amount": 15.50,
            "extra": 1.00,
            "mta_tax": 0.50,
            "tip_amount": 3.50,
            "tolls_amount": 0.00,
            "improvement_surcharge": 1.00,
            "total_amount": 21.50,
        }

        df = spark.createDataFrame([record, record])
        result = clean_yellow_taxi(df, 2024, 12)
        assert result.count() == 1


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not installed")
class TestBronzeToSilverWeather:
    """Tests for weather data cleaning."""

    def test_temperature_validation(self, spark):
        """Should filter out unreasonable temperatures."""
        from transformation.bronze_to_silver_weather import clean_weather

        data = [
            {
                "date": "2024-12-15",
                "temp_avg_celsius": 5.0,
                "temp_min_celsius": 2.0,
                "temp_max_celsius": 8.0,
                "precip_total_mm": 0.0,
                "wind_avg_ms": 3.0,
                "observation_count": 24,
            },
            # Invalid: 100°C in NYC
            {
                "date": "2024-12-16",
                "temp_avg_celsius": 100.0,
                "temp_min_celsius": 95.0,
                "temp_max_celsius": 105.0,
                "precip_total_mm": 0.0,
                "wind_avg_ms": 3.0,
                "observation_count": 24,
            },
        ]

        df = spark.createDataFrame(data)
        result = clean_weather(df, 2024)
        assert result.count() == 1

    def test_fahrenheit_conversion(self, spark):
        """Should correctly convert Celsius to Fahrenheit."""
        from transformation.bronze_to_silver_weather import clean_weather

        data = [
            {
                "date": "2024-12-15",
                "temp_avg_celsius": 0.0,
                "temp_min_celsius": -5.0,
                "temp_max_celsius": 5.0,
                "precip_total_mm": 0.0,
                "wind_avg_ms": 3.0,
                "observation_count": 24,
            },
        ]

        df = spark.createDataFrame(data)
        result = clean_weather(df, 2024)
        row = result.first()
        assert row["temp_avg_fahrenheit"] == pytest.approx(32.0, abs=0.1)

    def test_rainy_flag(self, spark):
        """Should set is_rainy=True when precipitation > 0.5mm."""
        from transformation.bronze_to_silver_weather import clean_weather

        data = [
            {
                "date": "2024-12-15",
                "temp_avg_celsius": 5.0,
                "temp_min_celsius": 2.0,
                "temp_max_celsius": 8.0,
                "precip_total_mm": 12.5,
                "wind_avg_ms": 5.0,
                "observation_count": 24,
            },
            {
                "date": "2024-12-16",
                "temp_avg_celsius": 5.0,
                "temp_min_celsius": 2.0,
                "temp_max_celsius": 8.0,
                "precip_total_mm": 0.0,
                "wind_avg_ms": 2.0,
                "observation_count": 24,
            },
        ]

        df = spark.createDataFrame(data)
        result = clean_weather(df, 2024)
        rows = {row["date"].isoformat(): row for row in result.collect()}
        assert rows["2024-12-15"]["is_rainy"] is True
        assert rows["2024-12-16"]["is_rainy"] is False

    def test_interpolates_single_day_temperature_gap(self, spark):
        """Should fill a null temp_avg using the mean of neighbor days."""
        from transformation.bronze_to_silver_weather import clean_weather

        # Interior null: 2024-12-16 has no temp_avg; neighbors are 4.0
        # and 8.0, so interpolation should yield (4.0 + 8.0) / 2 = 6.0.
        data = [
            {
                "date": "2024-12-15",
                "temp_avg_celsius": 4.0,
                "temp_min_celsius": 1.0,
                "temp_max_celsius": 7.0,
                "precip_total_mm": 0.0,
                "wind_avg_ms": 3.0,
                "observation_count": 24,
            },
            {
                "date": "2024-12-16",
                "temp_avg_celsius": None,
                "temp_min_celsius": 2.0,
                "temp_max_celsius": 9.0,
                "precip_total_mm": 0.0,
                "wind_avg_ms": 3.0,
                "observation_count": 24,
            },
            {
                "date": "2024-12-17",
                "temp_avg_celsius": 8.0,
                "temp_min_celsius": 3.0,
                "temp_max_celsius": 11.0,
                "precip_total_mm": 0.0,
                "wind_avg_ms": 3.0,
                "observation_count": 24,
            },
        ]

        df = spark.createDataFrame(data)
        result = clean_weather(df, 2024)
        rows = {row["date"].isoformat(): row for row in result.collect()}
        assert rows["2024-12-16"]["temp_avg_celsius"] == pytest.approx(6.0, abs=0.1)

    def test_is_snowy_requires_precip_and_freezing(self, spark):
        """Should set is_snowy only when precip > 0.5mm AND temp <= 1.0C."""
        from transformation.bronze_to_silver_weather import clean_weather

        data = [
            # Snowy: wet and freezing
            {
                "date": "2024-12-15",
                "temp_avg_celsius": 0.0,
                "temp_min_celsius": -3.0,
                "temp_max_celsius": 2.0,
                "precip_total_mm": 5.0,
                "wind_avg_ms": 3.0,
                "observation_count": 24,
            },
            # Rainy not snowy: wet but warm
            {
                "date": "2024-12-16",
                "temp_avg_celsius": 10.0,
                "temp_min_celsius": 6.0,
                "temp_max_celsius": 14.0,
                "precip_total_mm": 5.0,
                "wind_avg_ms": 3.0,
                "observation_count": 24,
            },
            # Neither: freezing but dry
            {
                "date": "2024-12-17",
                "temp_avg_celsius": -2.0,
                "temp_min_celsius": -5.0,
                "temp_max_celsius": 1.0,
                "precip_total_mm": 0.0,
                "wind_avg_ms": 3.0,
                "observation_count": 24,
            },
        ]

        df = spark.createDataFrame(data)
        result = clean_weather(df, 2024)
        rows = {row["date"].isoformat(): row for row in result.collect()}
        assert rows["2024-12-15"]["is_snowy"] is True
        assert rows["2024-12-16"]["is_snowy"] is False
        assert rows["2024-12-17"]["is_snowy"] is False

    def test_drops_rows_outside_requested_year(self, spark):
        """Should drop stray-year rows that would create bad partitions."""
        from transformation.bronze_to_silver_weather import clean_weather

        # This is the real bug we saw in gold: a stray-year row leaking
        # through. The year filter should drop the 2002 record.
        data = [
            {
                "date": "2024-12-15",
                "temp_avg_celsius": 5.0,
                "temp_min_celsius": 2.0,
                "temp_max_celsius": 8.0,
                "precip_total_mm": 0.0,
                "wind_avg_ms": 3.0,
                "observation_count": 24,
            },
            {
                "date": "2002-07-04",
                "temp_avg_celsius": 25.0,
                "temp_min_celsius": 20.0,
                "temp_max_celsius": 30.0,
                "precip_total_mm": 0.0,
                "wind_avg_ms": 3.0,
                "observation_count": 24,
            },
        ]

        df = spark.createDataFrame(data)
        result = clean_weather(df, 2024)
        years = {row["year"] for row in result.collect()}
        assert years == {2024}
