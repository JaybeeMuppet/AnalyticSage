# Assignment 1: Crypto Data Ingestion with Docker Compose

Pulls the top 100 coins by market cap from the CoinGecko API, cleans them with Pandas,
and loads them into Postgres (table `coin_markets`) using SQLAlchemy.

## Run
docker compose up --build

## Connect (DBeaver / pgcli)
Host: localhost | Port: 5433 | Database: crypto | User/Password: root / root

Note: I used port 5433 because I have a local Postgres already using port 5432.