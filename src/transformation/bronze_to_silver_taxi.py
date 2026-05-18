"""
Bronze → Silver: NYC Yellow Taxi Data
======================================
Cleans and standardizes raw taxi trip data:
- Casts columns to correct types
- Removes invalid records (null coordinates, impossible fares, etc.)
- Standardizes column names to snake_case
- Deduplicates
- Writes partitioned parquet to silver layer
- Registers table in Glue catalog

This script runs on EMR Serverless. It receives parameters via --conf flags
passed in the Airflow DAG.

Usage (local testing with spark-submit):
    spark-submit src/transformation/bronze_to_silver_taxi.py \
        --data-bucket my-bucket \
        --year 2024 --month 12 \
        --glue-database nyc_taxi_pipeline_silver_dev
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze_to_silver_taxi")


def create_spark_session(app_name: str = "bronze_to_silver_taxi") -> SparkSession:
    """Create SparkSession with Glue catalog integration."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, s3_path: str):
    """Read raw parquet from bronze layer."""
    logger.info(f"Reading bronze data from {s3_path}")
    df = spark.read.parquet(s3_path)
    logger.info(f"Bronze record count: {df.count()}")
    logger.info(f"Bronze schema: {df.columns}")
    return df


def clean_yellow_taxi(df):
    """
    Apply cleaning rules for yellow taxi data.

    Rules:
    1. Standardize column names to snake_case
    2. Cast types explicitly
    3. Filter out invalid records
    4. Add metadata columns
    5. Deduplicate
    """
    # ── Step 1: Standardize column names ──
    # TLC column names vary across years; normalize them
    column_mapping = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "RatecodeID": "rate_code_id",
        "store_and_fwd_flag": "store_and_fwd_flag",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "extra": "extra",
        "mta_tax": "mta_tax",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount": "total_amount",
        "congestion_surcharge": "congestion_surcharge",
        "airport_fee": "airport_fee",
    }

    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    # ── Step 2: Cast types ──
    df = (
        df.withColumn("vendor_id", F.col("vendor_id").cast(IntegerType()))
        .withColumn("pickup_datetime", F.col("pickup_datetime").cast(TimestampType()))
        .withColumn("dropoff_datetime", F.col("dropoff_datetime").cast(TimestampType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("rate_code_id", F.col("rate_code_id").cast(IntegerType()))
        .withColumn(
            "pickup_location_id", F.col("pickup_location_id").cast(IntegerType())
        )
        .withColumn(
            "dropoff_location_id", F.col("dropoff_location_id").cast(IntegerType())
        )
        .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
        .withColumn("extra", F.col("extra").cast(DoubleType()))
        .withColumn("mta_tax", F.col("mta_tax").cast(DoubleType()))
        .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
        .withColumn("tolls_amount", F.col("tolls_amount").cast(DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
    )

    # Handle columns that may not exist in older data
    if "congestion_surcharge" in df.columns:
        df = df.withColumn(
            "congestion_surcharge",
            F.col("congestion_surcharge").cast(DoubleType()),
        )
    else:
        df = df.withColumn("congestion_surcharge", F.lit(0.0))

    if "airport_fee" in df.columns:
        df = df.withColumn("airport_fee", F.col("airport_fee").cast(DoubleType()))
    else:
        df = df.withColumn("airport_fee", F.lit(0.0))

    # ── Step 3: Filter invalid records ──
    before_count = df.count()

    df = df.filter(
        # Must have pickup and dropoff times
        F.col("pickup_datetime").isNotNull()
        & F.col("dropoff_datetime").isNotNull()
        # Dropoff must be after pickup
        & (F.col("dropoff_datetime") > F.col("pickup_datetime"))
        # Reasonable trip distance (0-500 miles)
        & (F.col("trip_distance") >= 0)
        & (F.col("trip_distance") <= 500)
        # Reasonable fare (-$10 to $10,000) — slight negatives can be refunds
        & (F.col("fare_amount") >= -10)
        & (F.col("fare_amount") <= 10000)
        # Reasonable total amount
        & (F.col("total_amount") >= -10)
        & (F.col("total_amount") <= 10000)
        # Valid passenger count (0 can be valid for some records)
        & (F.col("passenger_count") >= 0)
        & (F.col("passenger_count") <= 9)
        # Valid location IDs (TLC zones are 1-263)
        & (F.col("pickup_location_id").between(1, 263))
        & (F.col("dropoff_location_id").between(1, 263))
    )

    after_count = df.count()
    logger.info(
        f"Filtered {before_count - after_count} invalid records "
        f"({(before_count - after_count) / max(before_count, 1) * 100:.1f}%)"
    )

    # ── Step 4: Add derived columns ──
    df = (
        df.withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_day_of_week", F.dayofweek("pickup_datetime"))
        .withColumn(
            "trip_duration_minutes",
            F.round(
                (
                    F.unix_timestamp("dropoff_datetime")
                    - F.unix_timestamp("pickup_datetime")
                )
                / 60.0,
                2,
            ),
        )
        .withColumn("taxi_type", F.lit("yellow"))
        # Partition columns
        .withColumn("year", F.year("pickup_datetime"))
        .withColumn("month", F.month("pickup_datetime"))
    )

    # ── Step 5: Deduplicate ──
    dedup_cols = [
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "fare_amount",
        "trip_distance",
    ]
    before_dedup = df.count()
    df = df.dropDuplicates(dedup_cols)
    after_dedup = df.count()
    logger.info(f"Removed {before_dedup - after_dedup} duplicate records")

    return df


def write_silver(df, output_path: str, glue_database: str, table_name: str):
    """Write cleaned data to silver layer as partitioned parquet."""
    logger.info(f"Writing silver data to {output_path}")

    (
        df.write.mode("overwrite")
        .partitionBy("year", "month")
        .option("path", output_path)
        .format("parquet")
        .saveAsTable(f"{glue_database}.{table_name}")
    )

    final_count = df.count()
    logger.info(f"Silver table written: {final_count} records")
    return final_count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-bucket", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--glue-database", required=True)
    args = parser.parse_args()

    spark = create_spark_session()

    try:
        # Read bronze
        bronze_path = (
            f"s3://{args.data_bucket}/bronze/nyc_tlc/yellow/"
            f"year={args.year}/month={args.month:02d}/"
        )
        df = read_bronze(spark, bronze_path)

        # Clean
        df_clean = clean_yellow_taxi(df)

        # Write silver
        silver_path = f"s3://{args.data_bucket}/silver/nyc_tlc/yellow/"
        write_silver(df_clean, silver_path, args.glue_database, "yellow_taxi_trips")

        logger.info("Bronze → Silver transformation complete!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
