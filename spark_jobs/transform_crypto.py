from session import get_spark
from pyspark.sql.functions import (col, lit, current_timestamp, to_timestamp, round as spark_round, when)
from pyspark.sql.types import FloatType, LongType
from datetime import datetime

today = datetime.utcnow().strftime("%Y-%m-%d")
BUCKET = "minidatalake"


spark = get_spark("transform_crypto")

raw_path = f"s3a://{BUCKET}/raw-data/Crypto/crypto/{today}.json"
df = spark.read.option("multiline", "true").json(raw_path)

df_clean = df.select(
    col("id").alias("coin_id"),
    col("symbol"),
    col("name"),
    col("current_price").cast(FloatType()).alias("price_usd"),
    col("market_cap").cast(LongType()),
    col("market_cap_rank").cast(LongType()).alias("rank"),
    col("total_volume").cast(LongType()).alias("volume_24h"),
    col("high_24h").cast(FloatType()),
    col("low_24h").cast(FloatType()),
    col("price_change_24h").cast(FloatType()),
    col("price_change_percentage_24h").cast(FloatType()).alias("price_change_24h_pct"),
    col("circulating_supply").cast(FloatType()),
    col("total_supply").cast(FloatType()),
    col("max_supply").cast(FloatType()),
    col("ath").cast(FloatType()),
    col("ath_change_percentage").cast(FloatType()),
    to_timestamp(col("ath_date")).alias("ath_date"),
    to_timestamp(col("last_updated")).alias("last_updated"),
    # roi is null for most coins — safely extract percentage only
    when(col("roi").isNotNull(), col("roi.percentage").cast(FloatType()))
    .otherwise(None).alias("roi_pct"),
) \
.filter(col("coin_id").isNotNull()) \
.filter(col("price_usd").isNotNull()) \
.filter(col("price_usd") > 0) \
.withColumn("price_usd", spark_round(col("price_usd"), 4)) \
.withColumn("price_change_24h_pct", spark_round(col("price_change_24h_pct"), 2)) \
.withColumn("ath_change_percentage", spark_round(col("ath_change_percentage"), 2)) \
.withColumn("ingestion_date", lit(today)) \
.withColumn("ingested_at", current_timestamp())

out_path = f"s3a://{BUCKET}/cleaned-data/Crypto/crypto/{today}"
df_clean.write.mode("overwrite").format("delta").save(out_path)
print(f"Crypto → {out_path} | Rows: {df_clean.count()}")

spark.stop()