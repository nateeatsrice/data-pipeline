"""
Silver → Gold: Feature Tables
===============================
Creates analytics-ready feature tables by joining taxi trips with weather data
and computing aggregations useful for data science projects.

Produces two gold tables:
1. trip_weather_daily — daily aggregated trip stats joined with weather
2. location_hourly_features — per-zone, per-hour features for demand prediction

These tables are designed to be directly consumable by ML pipelines.
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_to_gold")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("silver_to_gold_features")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def build_trip_weather_daily(spark, data_bucket: str, silver_db: str):
    """
    Gold Table 1: trip_weather_daily
    Joins daily taxi aggregates with weather to answer:
    - How does rain/snow affect ridership?
    - Do tips change with temperature?
    - What's the revenue impact of weather?
    """
    logger.info("Building trip_weather_daily feature table")

    # Read silver taxi data
    taxi = spark.table(f"{silver_db}.yellow_taxi_trips")

    # Daily taxi aggregation
    taxi_daily = taxi.groupBy("pickup_date").agg(
        F.count("*").alias("total_trips"),
        F.sum("passenger_count").alias("total_passengers"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration_min"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_amount").alias("avg_tip"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_total_amount"),
        # Tip percentage (exclude zero-fare trips)
        F.avg(
            F.when(
                F.col("fare_amount") > 0,
                F.col("tip_amount") / F.col("fare_amount") * 100,
            )
        ).alias("avg_tip_percentage"),
        # Payment type distribution
        F.sum(F.when(F.col("payment_type") == 1, 1).otherwise(0)).alias(
            "credit_card_trips"
        ),
        F.sum(F.when(F.col("payment_type") == 2, 1).otherwise(0)).alias("cash_trips"),
        # Time of day distribution
        F.sum(F.when(F.col("pickup_hour").between(6, 9), 1).otherwise(0)).alias(
            "morning_rush_trips"
        ),
        F.sum(F.when(F.col("pickup_hour").between(16, 19), 1).otherwise(0)).alias(
            "evening_rush_trips"
        ),
        F.sum(
            F.when(
                (F.col("pickup_hour") >= 22) | (F.col("pickup_hour") <= 5), 1
            ).otherwise(0)
        ).alias("late_night_trips"),
    )

    # Day of week features
    taxi_daily = taxi_daily.withColumn(
        "day_of_week", F.dayofweek("pickup_date")
    ).withColumn(
        "is_weekend",
        F.when(F.col("day_of_week").isin(1, 7), True).otherwise(False),
    )

    # Read silver weather data
    weather = spark.table(f"{silver_db}.nyc_weather_daily")
    weather_cols = weather.select(
        F.col("date").alias("weather_date"),
        "temp_avg_celsius",
        "temp_avg_fahrenheit",
        "temp_min_celsius",
        "temp_max_celsius",
        "precip_total_mm",
        "wind_avg_ms",
        "is_rainy",
        "is_snowy",
    )

    # Join taxi with weather on date
    features = taxi_daily.join(
        weather_cols,
        taxi_daily["pickup_date"] == weather_cols["weather_date"],
        "left",
    ).drop("weather_date")

    # Add partition columns
    features = features.withColumn("year", F.year("pickup_date")).withColumn(
        "month", F.month("pickup_date")
    )

    # Round all double columns to 2 decimal places for cleanliness
    double_cols = [
        f.name for f in features.schema.fields if isinstance(f.dataType, DoubleType)
    ]
    for col in double_cols:
        features = features.withColumn(col, F.round(F.col(col), 2))

    return features


def build_location_hourly_features(spark, data_bucket: str, silver_db: str):
    """
    Gold Table 2: location_hourly_features
    Per-zone, per-hour aggregations for demand prediction.
    Useful for: ride demand forecasting, surge pricing analysis,
    zone-level revenue optimization.
    """
    logger.info("Building location_hourly_features table")

    taxi = spark.table(f"{silver_db}.yellow_taxi_trips")

    location_hourly = taxi.groupBy(
        "pickup_date", "pickup_hour", "pickup_location_id"
    ).agg(
        F.count("*").alias("trip_count"),
        F.avg("trip_distance").alias("avg_distance"),
        F.avg("trip_duration_minutes").alias("avg_duration_min"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_amount").alias("avg_tip"),
        F.sum("total_amount").alias("total_revenue"),
        F.countDistinct("dropoff_location_id").alias("unique_destinations"),
    )

    # Add time features for ML
    location_hourly = (
        location_hourly.withColumn("day_of_week", F.dayofweek("pickup_date"))
        .withColumn(
            "is_weekend",
            F.when(F.col("day_of_week").isin(1, 7), True).otherwise(False),
        )
        .withColumn(
            "time_of_day",
            F.when(F.col("pickup_hour").between(6, 9), "morning_rush")
            .when(F.col("pickup_hour").between(10, 15), "midday")
            .when(F.col("pickup_hour").between(16, 19), "evening_rush")
            .when(F.col("pickup_hour").between(20, 21), "evening")
            .otherwise("late_night"),
        )
        .withColumn("year", F.year("pickup_date"))
        .withColumn("month", F.month("pickup_date"))
    )

    return location_hourly


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-bucket", required=True)
    parser.add_argument("--silver-database", required=True)
    parser.add_argument("--gold-database", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    spark = create_spark_session()

    try:
        # ── Gold Table 1: Trip + Weather Daily ──
        trip_weather = build_trip_weather_daily(
            spark, args.data_bucket, args.silver_database
        )

        gold_path_1 = f"s3://{args.data_bucket}/gold/features/trip_weather_daily/"
        (
            trip_weather.write.mode("overwrite")
            .partitionBy("year", "month")
            .option("path", gold_path_1)
            .format("parquet")
            .saveAsTable(f"{args.gold_database}.trip_weather_daily")
        )
        logger.info(f"trip_weather_daily: {trip_weather.count()} records written")

        # ── Gold Table 2: Location Hourly Features ──
        location_features = build_location_hourly_features(
            spark, args.data_bucket, args.silver_database
        )

        gold_path_2 = f"s3://{args.data_bucket}/gold/features/location_hourly_features/"
        (
            location_features.write.mode("overwrite")
            .partitionBy("year", "month")
            .option("path", gold_path_2)
            .format("parquet")
            .saveAsTable(f"{args.gold_database}.location_hourly_features")
        )
        logger.info(f"location_hourly_features: {location_features.count()} records")

        logger.info("Silver → Gold transformation complete!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
