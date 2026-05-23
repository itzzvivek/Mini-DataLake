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
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@localhost:5432/minidatalake")
engine = create_engine(DATABASE_URL)

@app.get("/")
def root():
    return {
        "status": "running",
        "pipeline": "APIs -> Airflow -> MinIO -> Spark -> PostgreSQL",
        "endpoints": ["/health", "/api/countries", "/api/crypto", "/api/weather", "/api/stats"],
    }

