"""
NYC TLC Taxi Data Ingestion
============================
Downloads yellow taxi trip parquet files from NYC TLC and uploads to S3 bronze layer.

The TLC publishes data monthly at:
    https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet

Data has a ~2 month publication lag (e.g., January data available in March).

Usage:
    # Ingest a specific month
    python -m src.ingestion.nyc_tlc_ingestion --year 2024 --month 12

    # Ingest the latest available month
    python -m src.ingestion.nyc_tlc_ingestion --latest
"""

import argparse
import logging
import sys
import tempfile
from pathlib import Path

import boto3
import requests

# Allow running as script or as module
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


def check_source_exists(url: str) -> bool:
    """Check if the source file exists via HEAD request (no download)."""
    try:
        response = requests.head(url, timeout=10, allow_redirects=True)
        return response.status_code == 200
    except Exception as e:
        logger.warning(f"HEAD request failed for {url}: {e}")
        return False


def check_already_ingested(s3_client, bucket: str, s3_key: str) -> bool:
    """Check if this file has already been ingested to avoid re-downloading."""
    try:
        s3_client.head_object(Bucket=bucket, Key=s3_key)
        return True
    except s3_client.exceptions.ClientError:
        return False


def download_file(url: str, local_path: Path) -> Path:
    """Download a file with progress logging."""
    logger.info(f"Downloading {url}")
    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    total_mb = total_size / (1024 * 1024)
    logger.info(f"Expected size: {total_mb:.1f} MB")
    downloaded = 0

    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)

    size_mb = downloaded / (1024 * 1024)
    logger.info(f"Downloaded {size_mb:.1f} MB to {local_path}")
    return local_path


def upload_to_s3(s3_client, local_path: Path, bucket: str, s3_key: str) -> str:
    """Upload a file to S3 and return the full S3 URI."""
    logger.info(f"Uploading to s3://{bucket}/{s3_key}")
    s3_client.upload_file(
        str(local_path),
        bucket,
        s3_key,
        ExtraArgs={"ContentType": "application/octet-stream"},
    )
    s3_uri = f"s3://{bucket}/{s3_key}"
    logger.info(f"Upload complete: {s3_uri}")
    return s3_uri


def ingest_yellow_taxi(
    year: int,
    month: int,
    config: Config = None,
    s3_client=None,
    skip_existing: bool = True,
) -> dict:
    """
    Ingest one month of yellow taxi data.

    Returns:
        dict with keys: success, s3_uri, year, month, skipped, error
    """
    config = config or Config()
    s3_client = s3_client or boto3.client("s3", region_name=config.AWS_REGION)

    result = {
        "success": False,
        "s3_uri": None,
        "year": year,
        "month": month,
        "skipped": False,
        "error": None,
    }

    # Build source URL and target S3 key
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    source_url = f"{config.NYC_TLC_BASE_URL}/{filename}"
    s3_key = (
        f"{config.BRONZE_PREFIX}/nyc_tlc/yellow/"
        f"year={year}/month={month:02d}/{filename}"
    )

    try:
        # Check if already ingested
        if skip_existing and check_already_ingested(
            s3_client, config.DATA_BUCKET, s3_key
        ):
            logger.info(f"Already ingested: {year}-{month:02d}. Skipping.")
            result["skipped"] = True
            result["success"] = True
            result["s3_uri"] = f"s3://{config.DATA_BUCKET}/{s3_key}"
            return result

        # Check if source exists
        if not check_source_exists(source_url):
            msg = (
                f"Source not available yet: {source_url}. TLC data has a ~2 month lag."
            )
            logger.warning(msg)
            result["error"] = msg
            return result

        # Download to temp file, then upload to S3
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / filename
            download_file(source_url, local_path)
            s3_uri = upload_to_s3(s3_client, local_path, config.DATA_BUCKET, s3_key)

        result["success"] = True
        result["s3_uri"] = s3_uri
        logger.info(f"Successfully ingested {year}-{month:02d}")

    except Exception as e:
        logger.error(f"Failed to ingest {year}-{month:02d}: {e}")
        result["error"] = str(e)

    return result


def ingest_green_taxi(
    year: int,
    month: int,
    config: Config = None,
    s3_client=None,
    skip_existing: bool = True,
) -> dict:
    """Ingest one month of green taxi data. Same pattern as yellow."""
    config = config or Config()
    s3_client = s3_client or boto3.client("s3", region_name=config.AWS_REGION)

    result = {
        "success": False,
        "s3_uri": None,
        "year": year,
        "month": month,
        "skipped": False,
        "error": None,
    }

    filename = f"green_tripdata_{year}-{month:02d}.parquet"
    source_url = f"{config.NYC_TLC_BASE_URL}/{filename}"
    s3_key = (
        f"{config.BRONZE_PREFIX}/nyc_tlc/green/year={year}/month={month:02d}/{filename}"
    )

    try:
        if skip_existing and check_already_ingested(
            s3_client, config.DATA_BUCKET, s3_key
        ):
            logger.info(f"Already ingested green {year}-{month:02d}. Skipping.")
            result["skipped"] = True
            result["success"] = True
            result["s3_uri"] = f"s3://{config.DATA_BUCKET}/{s3_key}"
            return result

        if not check_source_exists(source_url):
            logger.warning(f"Green taxi source not available: {source_url}")
            result["error"] = f"Source not available: {source_url}"
            return result

        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / filename
            download_file(source_url, local_path)
            s3_uri = upload_to_s3(s3_client, local_path, config.DATA_BUCKET, s3_key)

        result["success"] = True
        result["s3_uri"] = s3_uri

    except Exception as e:
        logger.error(f"Failed to ingest green {year}-{month:02d}: {e}")
        result["error"] = str(e)

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest NYC TLC yellow taxi data to S3 bronze layer"
    )
    parser.add_argument("--year", type=int, help="Year to ingest")
    parser.add_argument("--month", type=int, help="Month to ingest (1-12)")
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Ingest the latest available month",
    )
    parser.add_argument(
        "--include-green",
        action="store_true",
        help="Also ingest green taxi data",
    )
    args = parser.parse_args()

    config = Config()

    if args.latest:
        year, month = config.get_latest_available_month()
    elif args.year and args.month:
        year, month = args.year, args.month
    else:
        parser.error("Provide --year and --month, or --latest")

    result = ingest_yellow_taxi(year, month, config)
    print(f"Yellow: {result}")

    if args.include_green:
        result_green = ingest_green_taxi(year, month, config)
        print(f"Green: {result_green}")
