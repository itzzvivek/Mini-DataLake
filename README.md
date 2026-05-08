# Mini Data Lake House
An end-to-end data engineering pipeline that fetches real-time data from public APIs, processes it with PySpark, and serves it via PostgreSQL and Power BI.

# Architechure
Public APIs → Airflow → MinIO (Raw JSON) → Spark → MinIO (Parquet) → PostgreSQL → Power BI
