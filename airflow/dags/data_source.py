import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.minio_client import verify_minio_load
from airflow.providers.docker.operators.docker import DockerOperator 
from datasource_apis.source_apis import fetch_all_weather, fetch_news, fetch_crypto, fetch_countries


#MINIO Config
MINIO_HOST = f"{os.getenv('MINIO_HOST', 'minio')}:{os.getenv('MINIO_PORT', '9000')}"
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'minidatalake')

def spark_command(job_script):
    return (
        "spark-submit "
        "--master local[*] "
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
        f"/app/spark_jobs/{job_script}"
    )

default_args = {
    'owner': 'vivek',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='data_sources_dag',
    default_args=default_args,
    description='DAG to fetch data from various APIs and store in MinIO',
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['data_sources', 'minio']
) as dag:

    weather_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_all_weather
    )

    news_task = PythonOperator(
        task_id='fetch_news',
        python_callable=fetch_news
    )

    crypto_task = PythonOperator(
        task_id='fetch_crypto',
        python_callable=fetch_crypto
    )

    countries_task = PythonOperator(
        task_id='fetch_countries',
        python_callable=fetch_countries
    )

    verify_load_task = PythonOperator(
        task_id='verify_minio_load',
        python_callable=verify_minio_load
    )


    # Shared env vars for all spark containers -mathes session.py os.getenv() keys

    spark_env = {
        "SE_ENDPOINT": f"http://{os.getenv('MINIO_HOST', 'minio')}:{os.getenv('MINIO_PORT', '9000')}",
        "SE_ACCESS_KEY": os.getenv("MINIO_ROOT_USER", "minioadmin"),
        "SE_SECRET_KEY": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    }

    ## == TRANSFORMATION TASKS == ##
    transform_weather_task = DockerOperator(
        task_id="transform_weather",
        image = "spark-app:latest",
        api_version="auto",
        auto_remove=True,
        force_pull=False,
        command=spark_command("transform_weather.py"),
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment=spark_env,
        timeout=1800,
        execution_timeout=timedelta(minutes=30),
    )

    transform_news_task = DockerOperator(
        task_id="transform_news",
        image = "spark-app:latest",
        api_version="auto",
        auto_remove=True,
        force_pull=False,
        command=spark_command("transform_news.py"),
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment=spark_env,
        timeout=1800,
        execution_timeout=timedelta(minutes=30),
    )

    transform_crypto_task = DockerOperator(
        task_id="transform_crypto",
        image = "spark-app:latest",
        api_version="auto",
        auto_remove=True,
        force_pull=False,
        command=spark_command("transform_crypto.py"),
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment=spark_env,
        timeout=1800,
        execution_timeout=timedelta(minutes=30),
    )
 
    transform_countries_task = DockerOperator(
        task_id="transform_countries",
        image = "spark-app:latest", 
        api_version="auto",
        auto_remove=True,
        force_pull=False,
        command=spark_command("transform_countries.py"),
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment=spark_env,
        timeout=1800,
        execution_timeout=timedelta(minutes=30),
    )

    [weather_task, news_task, crypto_task, countries_task] >> verify_load_task

    verify_load_task >> [transform_weather_task, 
                         transform_news_task, 
                         transform_crypto_task, 
                         transform_countries_task
                        ]