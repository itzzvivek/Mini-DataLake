from session import get_spark
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp,
    arrays_zip, explode, when, round as spark_round
)
from pyspark.sql.types import FloatType
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime

today = datetime.utcnow().strftime("%Y-%m-%d")
BUCKET = "minidatalake"

spark = get_spark("transform_weather")

CITIES = ["Delhi", "London", "New York", "Tokyo", "Sydney"]
all_dfs = []

for city in CITIES:
    try:
        raw_path = f"s3a://{BUCKET}/raw-data/Weathers/weather_{city}/{today}.json"
        df = spark.read.option("multiline", "true").json(raw_path)

        # 1. Explode hourly arrays (time[], temperature_2m[], precipitation[]) into rows
        df_hourly = df.select(
            col("latitude"),
            col("longitude"),
            col("timezone"),
            col("elevation"),
            col("current_weather.temperature").alias("current_temp_c"),
            col("current_weather.windspeed").alias("current_windspeed_kmh"),
            col("current_weather.winddirection").alias("wind_direction_deg"),
            col("current_weather.weathercode").alias("weather_code"),
            col("current_weather.is_day").alias("is_day"),
            col("current_weather.time").alias("observation_time"),
            arrays_zip(
                col("hourly.time"),
                col("hourly.temperature_2m"),
                col("hourly.precipitation")
            ).alias("hourly_zipped")
        ).select(
            col("latitude"),
            col("longitude"),
            col("timezone"),
            col("elevation"),
            col("current_temp_c").cast(FloatType()),
            col("current_windspeed_kmh").cast(FloatType()),
            col("wind_direction_deg").cast(FloatType()),
            col("weather_code"),
            col("is_day"),
            to_timestamp(col("observation_time")).alias("observation_time"),
            explode(col("hourly_zipped")).alias("hourly_row")
        ).select(
            col("latitude"),
            col("longitude"),
            col("timezone"),
            col("elevation"),
            col("current_temp_c"),
            col("current_windspeed_kmh"),
            col("wind_direction_deg"),
            col("weather_code"),
            col("is_day"),
            col("observation_time"),
            to_timestamp(col("hourly_row.time")).alias("forecast_hour"),
            col("hourly_row.temperature_2m").cast(FloatType()).alias("temp_c"),
            col("hourly_row.precipitation").cast(FloatType()).alias("precipitation_mm"),
        )

        # 2. Map weathercode to human-readable description
        df_hourly = df_hourly.withColumn(
            "weather_description",
            when(col("weather_code") == 0, "Clear sky")
            .when(col("weather_code") == 1, "Mainly clear")
            .when(col("weather_code") == 2, "Partly cloudy")
            .when(col("weather_code") == 3, "Overcast")
            .when(col("weather_code").isin(45, 48), "Fog")
            .when(col("weather_code").isin(51, 53, 55), "Drizzle")
            .when(col("weather_code").isin(61, 63, 65), "Rain")
            .when(col("weather_code").isin(71, 73, 75), "Snow")
            .when(col("weather_code").isin(80, 81, 82), "Showers")
            .when(col("weather_code") == 95, "Thunderstorm")
            .otherwise("Unknown")
        )

        # 3. Filter bad records + add metadata
        df_clean = df_hourly \
            .filter(col("temp_c").isNotNull()) \
            .filter(col("temp_c") > -90) \
            .filter(col("temp_c") < 60) \
            .filter(col("precipitation_mm") >= 0) \
            .filter(col("forecast_hour").isNotNull()) \
            .withColumn("city", lit(city)) \
            .withColumn("temp_c", spark_round(col("temp_c"), 1)) \
            .withColumn("precipitation_mm", spark_round(col("precipitation_mm"), 2)) \
            .withColumn("ingestion_date", lit(today)) \
            .withColumn("ingested_at", current_timestamp())

        all_dfs.append(df_clean)
        print(f"{city}: {df_clean.count()} hourly rows")

    except Exception as e:
        print(f"Failed for {city}: {e}")
        raise

# 4. Union all cities → single Delta table
final_df = reduce(DataFrame.union, all_dfs)
out_path = f"s3a://{BUCKET}/cleaned-data/Weathers/{today}"
final_df.write.mode("overwrite").format("delta").save(out_path)
print(f"\n All weather → {out_path} | Total rows: {final_df.count()}")

spark.stop()