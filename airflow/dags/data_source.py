from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.minio_client import verify_minio_load
from datasource_apis.source_apis import fetch_all_weather, fetch_news, fetch_crypto, fetch_countries


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

    [weather_task, news_task, crypto_task, countries_task] >> verify_load_task