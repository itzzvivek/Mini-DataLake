from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.utils.minio_client import upload_to_minio

import os
import requests
import json


CITIES = {
    "Delhi":    {"lat": 28.7041, "lon": 77.1025},
    "London":   {"lat": 51.5074, "lon": -0.1278},
    "New York": {"lat": 40.7128, "lon": -74.0060},
    "Tokyo":    {"lat": 35.6895, "lon": 139.6917},
    "Sydney":   {"lat": -33.8688, "lon": 151.2093}
}

def fetch_all_weather():
    for city, coords in CITIES.items():
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={coords['lat']}&longitude={coords['lon']}"
            f"&hourly=temperature_2m,precipitation"
            f"&current_weather=true&timezone=auto"
        )
        response = requests.get(url)
        upload_to_minio(f"weather_{city}", response.json())
        print(f"Fetched weather data for {city}: {response.status_code}")

    def fetch_news():
        NEWS_API_KEY = os.getenv("NEWSAPI_KEY")
        url = f"https://newsdata.io/api/1/latest?apikey={NEWS_API_KEY}&q=all"
        response = requests.get(url)
        upload_to_minio("news", response.json())
        print(f"Fetched news data: {response.status_code}")

    def fetch_crypto():
        CRYPTOP_API_KEY = os.getenv("COINGECKO_API")
        headers = {"Accept": "application/json"}
        if CRYPTOP_API_KEY:
            headers["x-cg-demo-api-key"] = CRYPTOP_API_KEY
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 10, "page": 1}
        response = requests.get(url, headers=headers, params=params)
        upload_to_minio("crypto", response.json())
        print(f"Fetched crypto data: {response.status_code}")

    def fetch_countries():
        url = "https://restcountries.com/v3.1/all"
        response = requests.get(url)
        upload_to_minio("countries", response.json())
        print(f"Fetched countries data: {response.status_code}")