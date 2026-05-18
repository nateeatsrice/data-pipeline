"""
Test Fixtures
=============
Shared fixtures for all tests. Uses moto to mock AWS services
so tests run without any AWS credentials or costs.
"""

import os
import sys
from unittest.mock import MagicMock

import boto3
import pytest

# Add src to path so imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# Set test environment variables before any imports
os.environ["AWS_REGION"] = "us-east-1"
os.environ["DATA_BUCKET"] = "test-data-lake"
os.environ["SCRIPTS_BUCKET"] = "test-scripts"
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def mock_s3_client():
    """Create a mocked S3 client using moto."""
    try:
        from moto import mock_aws

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            # Create test buckets
            client.create_bucket(Bucket="test-data-lake")
            client.create_bucket(Bucket="test-scripts")
            yield client
    except ImportError:
        # If moto is not installed, use a basic mock
        client = MagicMock()
        yield client


@pytest.fixture
def test_config():
    """Create a test configuration."""
    from config import Config

    config = Config()
    config.DATA_BUCKET = "test-data-lake"
    config.SCRIPTS_BUCKET = "test-scripts"
    config.AWS_REGION = "us-east-1"
    return config


@pytest.fixture
def sample_taxi_data():
    """Create sample taxi data as a dictionary (for testing without Spark)."""
    return [
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
            "congestion_surcharge": 2.50,
            "airport_fee": 0.00,
        },
        {
            "VendorID": 2,
            "tpep_pickup_datetime": "2024-12-15 09:00:00",
            "tpep_dropoff_datetime": "2024-12-15 09:30:00",
            "passenger_count": 1,
            "trip_distance": 8.2,
            "RatecodeID": 1,
            "store_and_fwd_flag": "N",
            "PULocationID": 132,
            "DOLocationID": 48,
            "payment_type": 2,
            "fare_amount": 28.00,
            "extra": 0.00,
            "mta_tax": 0.50,
            "tip_amount": 0.00,
            "tolls_amount": 6.55,
            "improvement_surcharge": 1.00,
            "total_amount": 36.05,
            "congestion_surcharge": 2.50,
            "airport_fee": 0.00,
        },
    ]


@pytest.fixture
def sample_weather_data():
    """Create sample daily weather data."""
    return [
        {
            "date": "2024-12-15",
            "temp_avg_celsius": 2.3,
            "temp_min_celsius": -1.2,
            "temp_max_celsius": 5.8,
            "precip_total_mm": 0.0,
            "wind_avg_ms": 3.4,
            "observation_count": 24,
        },
        {
            "date": "2024-12-16",
            "temp_avg_celsius": 5.1,
            "temp_min_celsius": 1.0,
            "temp_max_celsius": 8.3,
            "precip_total_mm": 12.5,
            "wind_avg_ms": 5.8,
            "observation_count": 24,
        },
    ]
