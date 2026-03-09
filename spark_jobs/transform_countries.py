from session import get_spark
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime

today = datetime.utcnow().strftime("%Y-%m-%d")
BUCKET = "minidatalake"

spark = get_spark("transform_countries")

# todo: add file name
raw_path = f"s3a://{BUCKET}/raw-data/Countries/countries/{today}.json"
df = spark.read.option("multiline", "true").json(raw_path)

df_clean = df.select(
    col("name.common").alias("country_name"),
    col("name.official").alias("official_name"),
    col("region"),
    col("subregion"),
    col("population"),
    col("area"),
    col("timezones").getItem("0").alias("primary_timezone")
) \
.filter(col("country_name").isNotNull())\
.filter(col("population").isNotNull())\
.filter(col("population") >= 0)\
.filter(col("area").isNotNull())\
.withColumn("ingestion_date", lit(today)) \
.withColumn("ingestion_timestamp", current_timestamp())

out_path = f"s3a://{BUCKET}/cleaned-data/Countries/countries/{today}"
df_clean.write.mode("overwrite").format("delta").save(out_path)
print(f"Countries -> {out_path} | Rows: {df_clean.count()}")

spark.stop()