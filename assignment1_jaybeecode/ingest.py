import os
import sys
import time

import pandas as pd
import requests
from sqlalchemy import create_engine

# --- Config (read from environment, with local defaults) ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "root")
DB_NAME = os.getenv("DB_NAME", "crypto")
TABLE_NAME = "coin_markets"

API_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 100, "page": 1}

COLUMNS = [
    "id", "symbol", "name", "current_price", "market_cap", "market_cap_rank",
    "total_volume", "price_change_percentage_24h", "last_updated",
]


def extract(retries=3):
    """Fetch top-100 coins from CoinGecko, retrying on failure (e.g. rate limits)."""
    for attempt in range(1, retries + 1):
        try:
            print(f"Fetching CoinGecko data (attempt {attempt})...")
            resp = requests.get(API_URL, params=PARAMS, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            print(f"Fetched {len(data)} coins.")
            return data
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            if attempt < retries:
                time.sleep(5 * attempt)  # wait longer each retry
    sys.exit("Could not fetch data from CoinGecko after retries. Exiting.")


def transform(data):
    """Clean the raw API records into a tidy DataFrame."""
    df = pd.DataFrame(data)
    df = df[COLUMNS]                                        # keep only what we need
    df["symbol"] = df["symbol"].str.upper()                 # btc -> BTC
    df["last_updated"] = pd.to_datetime(df["last_updated"], utc=True)
    df = df.dropna(subset=["current_price"])                # a coin with no price is useless
    df = df.drop_duplicates(subset=["id"])                  # one row per coin
    df["ingested_at"] = pd.Timestamp.now(tz="UTC")          # when WE loaded it
    return df


def load(df):
    """Write the DataFrame to Postgres."""
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print(f"Connecting to Postgres at {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    engine = create_engine(url)
    df.to_sql(TABLE_NAME, con=engine, if_exists="replace", index=False)
    print(f"Loaded {len(df)} rows into '{TABLE_NAME}'.")


if __name__ == "__main__":
    raw = extract()
    clean = transform(raw)
    print(clean.head())
    load(clean)
    print("Ingestion complete!")