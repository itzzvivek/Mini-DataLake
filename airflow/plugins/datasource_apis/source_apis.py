import os
import requests
from utils.minio_client import upload_to_minio
from dotenv import load_dotenv

load_dotenv()

CITIES = {
    "Delhi":    {"lat": 28.7041, "lon": 77.1025},
    "London":   {"lat": 51.5074, "lon": -0.1278},
    "New York": {"lat": 40.7128, "lon": -74.0060},
    "Tokyo":    {"lat": 35.6895, "lon": 139.6917},
    "Sydney":   {"lat": -33.8688, "lon": 151.2093}
}

def fetch_all_weather():
    for city, coords in CITIES.items():
        try:
            url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={coords['lat']}&longitude={coords['lon']}"
                f"&hourly=temperature_2m,precipitation"
                f"&current_weather=true&timezone=auto"
            )
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            upload_to_minio(f"Weathers/weather_{city}", response.json())
            print(f"Fetched weather for {city}: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch weather for {city}: {e}")
            raise

def fetch_news():
    try:
        NEWS_API_KEY = os.getenv("NEWSAPI_KEY")
        if not NEWS_API_KEY:
            raise ValueError("NEWSAPI_KEY env var is not set")
        url = f"https://newsdata.io/api/1/latest?apikey={NEWS_API_KEY}&q=all"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        upload_to_minio("News/news", response.json())
        print(f"Fetched news data: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch news: {e}")
        raise

def fetch_crypto():
    try:
        CRYPTO_API_KEY = os.getenv("COINGECKO_API")
        headers = {"Accept": "application/json"}
        if CRYPTO_API_KEY:
            headers["x-cg-demo-api-key"] = CRYPTO_API_KEY
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 10, "page": 1}
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        upload_to_minio("Crypto/crypto", response.json())
        print(f"Fetched crypto data: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch crypto: {e}")
        raise

def fetch_countries():
    try:
        url = "https://restcountries.com/v3.1/all"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        upload_to_minio("Countries/countries", response.json())
        print(f"Fetched countries data: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch countries: {e}")
        raise