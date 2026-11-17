# Assignment 4: Uniswap V3 swaps modelled with dbt

Uniswap V3 swaps from Ethereum mainnet, loaded into Postgres and modelled with dbt in three layers: staging, intermediate and marts.

## The dataset

Every Swap event from three Uniswap V3 pools on Ethereum mainnet, between blocks **20,000,000 and 20,000,199** (June 2024):

| Pool | Address | Fee |
| --- | --- | --- |
| ETH/USDC | `0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640` | 0.05% |
| WBTC/USDC | `0x99ac8cA7087fA4A2A1FB6357269965A2014ABc35` | 0.30% |
| USDC/USDT | `0x3416cF6C708Da44DB2624D63ea0AAef7113527C6` | 0.01% |

The data comes from my own extractor from Assignment 2. A snapshot of the raw swaps is committed in `data/uniswap_v3_swaps_snapshot.csv`, so the pipeline runs without an RPC connection.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with Docker Compose
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (installs Python 3.12 and all dependencies for you)
- Git
- Port **5434** free on your machine (change `DB_PORT` in `.env` if not)

## Run from scratch

```bash
git clone https://github.com/JaybeeMuppet/AnalyticSage.git
cd AnalyticSage/assignment4_jaybeecode

cp .env.example .env              # fill DB_*, never commit .env
set -a && source .env && set +a   # export the variables for dbt
docker compose up -d              # Postgres
uv sync                           # dependencies
uv run extract/load_swaps.py      # raw data (snapshot) → Postgres
uv run dbt build                  # seed + models + tests, all green
```

`.env.example` works as it is for a local run. Change `DB_PASS` if you like.

The `set -a && source .env && set +a` line exports the variables in `.env` so dbt can read them through `env_var()` in `profiles.yml`. Docker Compose and the extractor read `.env` by themselves. Run it again if you open a new terminal.

Check Postgres is healthy before extracting: `docker compose ps` should show `healthy`.

## Project structure

```
assignment4_jaybeecode/
├── docker-compose.yml        Postgres, configured from .env
├── .env.example              every variable needed (copy to .env)
├── pyproject.toml / uv.lock  dependencies (dbt-core, dbt-postgres, web3, psycopg)
├── dbt_project.yml
├── profiles.yml              Postgres target, all settings from env vars
├── data/
│   └── uniswap_v3_swaps_snapshot.csv   committed raw data
├── extract/
│   ├── load_swaps.py         idempotent loader (snapshot or RPC)
│   └── README.md
├── seeds/
│   └── uniswap_v3_pools.csv  pool tokens, decimals and fee tiers
├── models/
│   ├── staging/              clean, cast, rename only
│   ├── intermediate/         business logic
│   └── marts/                fct_uniswap_v3_swaps
└── tests/                    singular data tests
```

## The models

```
source: raw_uniswap_v3_swaps ──► stg_uniswap_v3_swaps ──┐
                                                        ├──► int_uniswap_v3_swaps_enriched ──► fct_uniswap_v3_swaps
seed:   uniswap_v3_pools ──────► stg_uniswap_v3_pools ──┘
```

**Staging** (views): `stg_uniswap_v3_swaps` reads the raw table through `source()`, the only place `source()` is used. It casts types, lowercases addresses and renames columns (e.g. `amount0` → `amount0_raw`). `stg_uniswap_v3_pools` does the same for the pool seed. No business logic.

**Intermediate** (view): `int_uniswap_v3_swaps_enriched` holds the decisions:
- Joins each swap to its pool's token symbols and decimals
- **Direction**: a positive amount means that token went into the pool, so the trader sold it. `token0_to_token1` or `token1_to_token0`
- **Token in / token out** with amounts in human units, always positive
- **Price**: decodes `sqrtPriceX96` into the price of the non-USDC token in USDC, adjusted for decimals
- **Volume in USDC**: the size of the USDC leg (all three pools have one)
- **Trade size bucket**: `small` (< 1k), `medium` (< 10k), `large` (< 100k), `whale` (≥ 100k USDC)

**Marts** (table): `fct_uniswap_v3_swaps`, materialized as a table, reads only the intermediate model.
**Grain: one row per swap** (`swap_id` = `tx_hash`-`log_index`). It is the table analysts and BI tools query; daily volume, per-pool or per-wallet views are built on top of it.

## Tests

- One `schema.yml` per layer: a description for every model and column, and `not_null` on every column
- `accepted_values` on the derived columns `direction` and `trade_size_bucket`, and on `pool_name`
- `unique` on `swap_id` in the mart and on `pool_address` in the seed
- Singular tests in `tests/`: every swap matches a known pool (so no swap silently disappears in the join), and the mart has exactly as many rows as staging

## Design notes

- **Idempotent load**: the raw table's primary key is `(tx_hash, log_index)` with `ON CONFLICT DO NOTHING`. Running the extract twice adds 0 rows.
- **Exact numbers**: raw amounts and `sqrtPriceX96` are up to 256-bit integers, stored as `NUMERIC(78,0)`. The price maths uses `numeric`, not floating point.
- **Port as a number**: `profiles.yml` uses `env_var('DB_PORT') | as_number`. Without it, dbt rejects the port, because `env_var()` always returns text.

## Useful queries

```sql
-- Volume and trade count per pool
select pool_name, count(*) as trades, round(sum(volume_usdc), 2) as volume_usdc
from fct_uniswap_v3_swaps
group by pool_name
order by volume_usdc desc;

-- Trades by size
select trade_size_bucket, count(*) as trades
from fct_uniswap_v3_swaps
group by trade_size_bucket;
```

## Clean up

```bash
docker compose down -v   # stop Postgres and delete its data
```
