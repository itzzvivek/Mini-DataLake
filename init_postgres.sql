-- Weather
CREATE TABLE IF NOT EXISTS weather (
    city VARCHAR(50),
    forecast_hour TIMESTAMP,
    temp_c FLOAT,
    precipitation_mm FLOAT,
    current_temp_c FLOAT,
    current_windspeed_kmh FLOAT,
    wind_direction_deg FLOAT,
    weather_code INT,
    weather_description VARCHAR(50),
    is_day INT,
    observation_time TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    timezone VARCHAR(50),
    elevation FLOAT,
    ingestion_date VARCHAR(20),
    ingested_at TIMESTAMP
);

-- News
CREATE TABLE IF NOT EXISTS news (
    article_id VARCHAR(100),
    title TEXT,
    description TEXT,
    url TEXT,
    source VARCHAR(100),
    source_name VARCHAR(100),
    source_priority INT,
    language VARCHAR(20),
    country VARCHAR(100),
    category VARCHAR(100),
    keywords TEXT,
    published_at TIMESTAMP,
    is_duplicate BOOLEAN,
    ingestion_date VARCHAR(20),
    ingested_at TIMESTAMP
);

-- Crypto
CREATE TABLE IF NOT EXISTS crypto (
    coin_id VARCHAR(50),
    symbol VARCHAR(20),
    name VARCHAR(50),
    price_usd FLOAT,
    market_cap BIGINT,
    rank BIGINT,
    volume_24h BIGINT,
    high_24h FLOAT,
    low_24h FLOAT,
    price_change_24h FLOAT,
    price_change_24h_pct FLOAT,
    circulating_supply FLOAT,
    total_supply FLOAT,
    max_supply FLOAT,
    ath FLOAT,
    ath_change_percentage FLOAT,
    ath_date TIMESTAMP,
    last_updated TIMESTAMP,
    roi_pct FLOAT,
    ingestion_date VARCHAR(20),
    ingested_at TIMESTAMP
);

-- Countries
CREATE TABLE IF NOT EXISTS countries (
    country_name VARCHAR(100),
    official_name VARCHAR(200),
    region VARCHAR(50),
    subregion VARCHAR(100),
    population BIGINT,
    area FLOAT,
    primary_timezone VARCHAR(50),
    flag_url TEXT,
    ingestion_date VARCHAR(20),
    ingested_at TIMESTAMP
);