"""
NOAA Weather Data Ingestion
============================
Downloads hourly weather observations for NYC (Central Park station)
from NOAA's Global Hourly dataset and uploads to S3 bronze layer.

NOAA data is available at:
    https://www.ncei.noaa.gov/data/global-hourly/access/{YEAR}/{STATION_ID}.csv

Usage:
    python -m src.ingestion.noaa_weather_ingestion --year 2024
"""

import argparse
import io
import logging
import sys
import tempfile
from pathlib import Path

import boto3
import pandas as pd
import requests

try:
    from src.config import Config
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from src.config import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def parse_noaa_temperature(tmp_col: pd.Series) -> pd.Series:
    """
    Parse NOAA TMP field: format is '+0123,1' meaning +12.3°C, quality code 1.
    Returns temperature in Celsius as float.
    """

    def _parse(val):
        if pd.isna(val) or val == "+9999,9":
            return None
        try:
            temp_str = str(val).split(",")[0]
            return float(temp_str) / 10.0
        except (ValueError, IndexError):
            return None

    return tmp_col.apply(_parse)


def parse_noaa_precipitation(aa1_col: pd.Series) -> pd.Series:
    """
    Parse NOAA AA1 (liquid precipitation) field.
    Format: 'HH,DDDD,C,Q' -> period hours, depth in mm*10, condition, quality.
    Returns precipitation in mm.
    """

    def _parse(val):
        if pd.isna(val):
            return 0.0
        try:
            parts = str(val).split(",")
            if len(parts) >= 2:
                depth = float(parts[1]) / 10.0
                return depth if depth < 999 else 0.0
        except (ValueError, IndexError):
            pass
        return 0.0

    return aa1_col.apply(_parse)


def download_noaa_data(year: int, station_id: str) -> pd.DataFrame:
    """Download and parse NOAA hourly data for a given year and station."""
    url = f"https://www.ncei.noaa.gov/data/global-hourly/access/{year}/{station_id}.csv"
    logger.info(f"Downloading NOAA data from {url}")

    response = requests.get(url, timeout=120)
    response.raise_for_status()

    df = pd.read_csv(
        io.StringIO(response.text),
        low_memory=False,
        dtype=str,  # Read everything as string initially
    )

    logger.info(f"Downloaded {len(df)} hourly observations for {year}")
    return df


def process_noaa_to_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate hourly NOAA observations to daily summaries.
    Extracts: date, avg/min/max temperature, total precipitation, avg wind speed.
    """
    # Parse the DATE column
    df["datetime"] = pd.to_datetime(df["DATE"], errors="coerce")
    df["date"] = df["datetime"].dt.date

    # Parse temperature
    if "TMP" in df.columns:
        df["temp_celsius"] = parse_noaa_temperature(df["TMP"])
    else:
        df["temp_celsius"] = None

    # Parse wind speed (WND field: angle, quality, type, speed*10, quality)
    if "WND" in df.columns:

        def parse_wind(val):
            if pd.isna(val):
                return None
            try:
                parts = str(val).split(",")
                if len(parts) >= 4:
                    speed = float(parts[3]) / 10.0  # m/s
                    return speed if speed < 999 else None
            except (ValueError, IndexError):
                return None

        df["wind_speed_ms"] = df["WND"].apply(parse_wind)
    else:
        df["wind_speed_ms"] = None

    # Parse precipitation
    if "AA1" in df.columns:
        df["precip_mm"] = parse_noaa_precipitation(df["AA1"])
    else:
        df["precip_mm"] = 0.0

    # Aggregate to daily
    daily = (
        df.groupby("date")
        .agg(
            temp_avg_celsius=("temp_celsius", "mean"),
            temp_min_celsius=("temp_celsius", "min"),
            temp_max_celsius=("temp_celsius", "max"),
            precip_total_mm=("precip_mm", "sum"),
            wind_avg_ms=("wind_speed_ms", "mean"),
            observation_count=("datetime", "count"),
        )
        .reset_index()
    )

    # Round numeric columns
    for col in [
        "temp_avg_celsius",
        "temp_min_celsius",
        "temp_max_celsius",
        "wind_avg_ms",
    ]:
        daily[col] = daily[col].round(1)
    daily["precip_total_mm"] = daily["precip_total_mm"].round(1)

    logger.info(f"Aggregated to {len(daily)} daily records")
    return daily


def ingest_weather(
    year: int,
    config: Config = None,
    s3_client=None,
    skip_existing: bool = True,
) -> dict:
    """
    Ingest one year of NOAA weather data.

    Returns:
        dict with keys: success, s3_uri, year, skipped, error, record_count
    """
    config = config or Config()
    s3_client = s3_client or boto3.client("s3", region_name=config.AWS_REGION)

    result = {
        "success": False,
        "s3_uri": None,
        "year": year,
        "skipped": False,
        "error": None,
        "record_count": 0,
    }

    filename = f"nyc_weather_daily_{year}.parquet"
    s3_key = f"{config.BRONZE_PREFIX}/noaa_weather/nyc_daily/year={year}/{filename}"

    try:
        # Check if already ingested
        if skip_existing and _check_exists(s3_client, config.DATA_BUCKET, s3_key):
            logger.info(f"Weather {year} already ingested. Skipping.")
            result["skipped"] = True
            result["success"] = True
            result["s3_uri"] = f"s3://{config.DATA_BUCKET}/{s3_key}"
            return result

        # Download raw hourly data
        raw_df = download_noaa_data(year, config.NOAA_STATION_ID)

        # Aggregate to daily
        daily_df = process_noaa_to_daily(raw_df)
        result["record_count"] = len(daily_df)

        # Save as parquet and upload
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / filename
            daily_df.to_parquet(local_path, index=False, engine="pyarrow")

            logger.info(f"Uploading to s3://{config.DATA_BUCKET}/{s3_key}")
            s3_client.upload_file(str(local_path), config.DATA_BUCKET, s3_key)

        result["success"] = True
        result["s3_uri"] = f"s3://{config.DATA_BUCKET}/{s3_key}"
        logger.info(f"Successfully ingested weather for {year}")

    except Exception as e:
        logger.error(f"Failed to ingest weather {year}: {e}")
        result["error"] = str(e)

    return result


def _check_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.ClientError:
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest NOAA weather data to S3 bronze layer"
    )
    parser.add_argument("--year", type=int, required=True, help="Year to ingest")
    args = parser.parse_args()

    result = ingest_weather(args.year)
    print(f"Weather ingestion: {result}")
