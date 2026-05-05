import os
from session import get_spark
from datetime import datetime

today = os.getenv('TRANSFORM_DATE', datetime.utcnow().strftime('%Y-%m-%d'))
BUCKET = "minidatalake"

POSTGRES_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:5432/{os.getenv('POSTGRES_DB', 'minidatalake')}"

POSTGRES_PROPS = {
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv('POSTGRES_PASSWORD', 'airflow'),
    "driver": "org.postgresql.Driver"
}

spark = get_spark("load_to_postgres")

datasets = [
    ("Weathers", "weather"),
    ("News/news", "news"),
    ("Crypto/crypto", "crypto"),
    ("Countries/countries", "countries"),
]

for folder, table in datasets:
    try:
        path = f"s3a://{BUCKET}/cleaned-data/{folder}/{today}"
        df = spark.read.format("parquet").load(path)

        df.write.jdbc(POSTGRES_URL, table, mode='append', properties=POSTGRES_PROPS)
        print(f"✅ Loaded {table} → PostgreSQL | Rows: {df.count()}")

    except Exception as e:
        print(f"❌ Error loading {table}: {e}")
        raise

spark.stop()