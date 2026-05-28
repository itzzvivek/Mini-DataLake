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
            