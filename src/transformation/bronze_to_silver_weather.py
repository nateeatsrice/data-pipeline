"""
Bronze → Silver: NOAA Weather Data
====================================
Standardizes and validates daily weather observations.

The bronze layer already has daily aggregated data (from the ingestion step).
Silver cleaning:
- Validates ranges (temperature, wind, precipitation)
- Fills small gaps via interpolation
- Adds derived columns (is_rainy, temp_fahrenheit)
- Registers in Glue catalog
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze_to_silver_weather")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("bronze_to_silver_weather")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def clean_weather(df):
    """Clean and validate weather data."""

    # Cast types
    df = (
        df.withColumn("date", F.col("date").cast(DateType()))
        .withColumn("temp_avg_celsius", F.col("temp_avg_celsius").cast(DoubleType()))
        .withColumn("temp_min_celsius", F.col("temp_min_celsius").cast(DoubleType()))
        .withColumn("temp_max_celsius", F.col("temp_max_celsius").cast(DoubleType()))
        .withColumn("precip_total_mm", F.col("precip_total_mm").cast(DoubleType()))
        .withColumn("wind_avg_ms", F.col("wind_avg_ms").cast(DoubleType()))
    )

    # Filter unreasonable values (NYC temperature range: -40 to 50°C)
    df = df.filter(
        F.col("date").isNotNull()
        & (
            F.col("temp_avg_celsius").isNull()
            | F.col("temp_avg_celsius").between(-40, 50)
        )
        & (F.col("precip_total_mm").isNull() | (F.col("precip_total_mm") >= 0))
        & (F.col("wind_avg_ms").isNull() | F.col("wind_avg_ms").between(0, 50))
    )

    # Interpolate small gaps in temperature using neighboring days
    window = Window.orderBy("date")
    df = df.withColumn(
        "temp_avg_celsius",
        F.when(
            F.col("temp_avg_celsius").isNull(),
            (
                F.lag("temp_avg_celsius", 1).over(window)
                + F.lead("temp_avg_celsius", 1).over(window)
            )
            / 2.0,
        ).otherwise(F.col("temp_avg_celsius")),
    )

    # Add derived columns
    df = (
        df.withColumn(
            "temp_avg_fahrenheit",
            F.round(F.col("temp_avg_celsius") * 9.0 / 5.0 + 32, 1),
        )
        .withColumn(
            "temp_min_fahrenheit",
            F.round(F.col("temp_min_celsius") * 9.0 / 5.0 + 32, 1),
        )
        .withColumn(
            "temp_max_fahrenheit",
            F.round(F.col("temp_max_celsius") * 9.0 / 5.0 + 32, 1),
        )
        .withColumn(
            "is_rainy",
            F.when(F.col("precip_total_mm") > 0.5, True).otherwise(False),
        )
        .withColumn(
            "is_snowy",
            F.when(
                (F.col("precip_total_mm") > 0.5) & (F.col("temp_avg_celsius") <= 1.0),
                True,
            ).otherwise(False),
        )
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
    )

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-bucket", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--glue-database", required=True)
    args = parser.parse_args()

    spark = create_spark_session()

    try:
        bronze_path = (
            f"s3://{args.data_bucket}/bronze/noaa_weather/nyc_daily/year={args.year}/"
        )
        logger.info(f"Reading bronze weather from {bronze_path}")
        df = spark.read.parquet(bronze_path)
        logger.info(f"Bronze weather records: {df.count()}")

        df_clean = clean_weather(df)

        silver_path = f"s3://{args.data_bucket}/silver/noaa_weather/nyc_daily/"
        logger.info(f"Writing silver weather to {silver_path}")

        (
            df_clean.write.mode("overwrite")
            .partitionBy("year", "month")
            .option("path", silver_path)
            .format("parquet")
            .saveAsTable(f"{args.glue_database}.nyc_weather_daily")
        )

        logger.info(f"Silver weather table written: {df_clean.count()} records")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
