from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import os

app = FastAPI(
    title="Mini Data Lake API",
    description="Real-time data from Countries, Crypto, and Weather APIs",
    version="1.0.0"
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@postgrest:5432/minidatalake")
engine = create_engine(DATABASE_URL)

@app.get("/")
def root():
    return {
        "status": "running",
        "pipeline": "APIs -> Airflow -> MinIO -> Spark -> PostgreSQL",
        "endpoints": ["/health", "/api/countries", "/api/crypto", "/api/weather", "/api/stats"],
    }


@app.get("/health")
def health_check():
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.get("/api/crypto/latest")
def get_crypto_latest(limit: int = 10):
    """Fetch the latest cryptocurrency data from the database."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT * FROM crypto ORDER BY ingested_at DESC LIMIT {limit}"
            ))
            data = [dict(row._mapping) for row in result]
        return {"status": "success", "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching crypto data: {str(e)}")

@app.get("/api/crypto/coin/{name}")
def get_crypto_by_coin(name: str, hours: int = 24):
    """Get specific coin data for last N hours"""
    try:
        since = datetime.now() - timedelta(hours=hours)
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT * FROM crypto WHERE name = :coin AND ingested_at >= :since ORDER BY ingested_at DESC",
            ), {"coin": name, "since": since})
            data = [dict(row._mapping) for row in result]
        return {"coin": name,"hours": hours, "count": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching crypto data: {str(e)}")

@app.get("/api/crypto/stats")
def get_crypto_stats(hours: int = 24):
    """Get Crypto statistics"""
    try:
        with engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM crypto")).scalar()
            latest = conn.execute(text("SELECT ingested_at FROM crypto ORDER BY ingested_at DESC LIMIT 1")).scalar()
            coins = conn.execute(text("SELECT DISTINCT name FROM crypto")).fetchall()
        return {
            "total_records": total,
            "latest_ingested_at": latest,
            "unique_coins": [row[0] for row in coins],
            "coins": [c[0] for c in coins]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching crypto stats: {str(e)}")
    
# Weather Endpoints

@app.get("/api/weather/latest")
def get_weather_latest(limit: int = 10):
    """Get latest weather data"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT * FROM weather ORDER BY ingested_at DESC LIMIT {limit}"
            ))
            data = [dict(row._mapping) for row in result]
        return {"count": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching weather data: {str(e)}")

@app.get("/api/weather/city/{city}")
def get_weather_by_city(city: str, hours: int = 24):
    """Get weather for specific city"""
    try:
        since = datetime.now() - timedelta(hours=hours)
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT * FROM weather WHERE city = :city AND ingested_at >= :since ORDER BY ingested_at DESC",
            ), {"city": city, "since": since})
            data = [dict(row._mapping) for row in result]
        return {"city": city,"hours": hours, "count": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching weather data: {str(e)}")

# Countries Endpoints
@app.get("/api/countries/latest")
def get_countries_latest(limit: int = 50):
    """Get latest countries data"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT * FROM countries ORDER BY ingestion_timestamp DESC LIMIT {limit}"
            ))
            data = [dict(row._mapping) for row in result]
        return {"count": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching countries data: {str(e)}")
    
@app.get("/api/countries/search/{country_name}")
def get_country_by_name(country_name: str):
    """Search for a specific country"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT * FROM countries WHERE country_name = :country_name ORDER BY ingestion_timestamp DESC LIMIT 10",
            ), {"country_name": country_name})
            data = [dict(row._mapping) for row in result]
        return {"query": country_name, "count": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching countries data: {str(e)}")

