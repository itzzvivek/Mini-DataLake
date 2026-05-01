import os
from session import get_spark
from datetime import datetime

today = os.getenv('TRANSFORM_DATE', datetime.utcnow().strftime('%Y-%m-%d'))
BUKCET  = "minidatalake"

POSTGRES_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:5432/{os.getenv('POSTGRES_DB', 'minidatalake')}"
POSTGRES_PROPS = {
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv('POSTGRES_PASSWORD', 'postgres'),
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
        path = f"s3a://{BUKCET}/cleaned-data/{folder}/{today}"
        df = spark.read.format("parquet").load(path)

        df.write.jdbc(POSTGRES_URL, table, mode='append', properties=POSTGRES_PROPS)
        print(f"Successfully loaded {table} data to PostgreSQL.")

    except Exception as e:
        print(f"Error loading {table} data: {e}")
        raise

spark.stop()

