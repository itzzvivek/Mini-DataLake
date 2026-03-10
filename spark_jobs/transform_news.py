from session import get_spark
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp, explode, array_join, length, wheb
)
from datetime import datetime

today = datetime.utcnow().strftime("%Y-%m-%d")
BUCKET = "minidatalake"

spark = get_spark("transform_news")

raw_path = f"s3a://{BUCKET}/raw-data/News/news/{today}.json"
df = spark.read.option("multiline", "true").json(raw_path)

#1. Explode result array
df_exploded = df.select(
    explode(col("result")).alias("a")
)

#2. Select useful fields - drop all paids -only fields
df_clean = df_exploded.select(
    col("a.article_id"),
    col("a.title"),
    col("a.description"),
    col("a.link").alias("url"),
    col("a.source_id").alias("source"),
    col("a.source_name"),
    col("a.source_priority"),
    col("a.language"),
    # country is array → join to string e.g. "united states of america"
    array_join(col("a.country"), ", ").alias("country"),
    # category is array → join to string e.g. "business, top"
    array_join(col("a.category"), ", ").alias("category"),
    # keywords is array → join to string
    array_join(col("a.keywords"), ", ").alias("keywords"),
    to_timestamp(col("a.pubDate"), "yyyy-MM-dd HH:mm:ss").alias("published_at"),
    col("a.duplicate").alias("is_duplicate"),
) \
.filter(col("title").isNotNull()) \
.filter(col("description").isNotNull()) \
.filter(~col("description").contains("ONLY AVAILABLE")) \
.filter(length(col("title")) > 5) \
.filter(col("is_duplicate") == False) \
.withColumn("ingestion_date", lit(today)) \
.withColumn("ingested_at", current_timestamp())

out_path = f"s3a://{BUCKET}/cleaned-data/News/news/{today}"
df_clean.write.mode("overwrite").format("delta").save(out_path)
print(f"✅ News → {out_path} | Unique articles: {df_clean.count()}")

spark.stop()
