import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.7.3",
]

def get_spark(app_name: str = "incremental_data"):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")

        # ---- S3A / MinIO ----
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("S3_ENDPOINT", "http://localhost:9000")
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # ---- S3A timeouts (numeric only) ----
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")

        # ---- Delta Lake ----
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config(
            "spark.databricks.delta.schema.autoMerge.enabled",
            "true"
        )
    )

    spark = configure_spark_with_delta_pip(
        builder,
        extra_packages=PACKAGES
    ).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("Spark started successfully")

    return spark