# Assignment 2: Uniswap V3 Swap Extraction Pipeline

A Python pipeline that pulls Uniswap V3 `Swap` events for three Ethereum mainnet pools over JSON-RPC, decodes them with web3.py, and loads them into Postgres. Re-running the same block range never creates duplicate rows.

## Pools

| Pool | Address |
| --- | --- |
| ETH/USDC | `0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640` |
| WBTC/USDC | `0x99ac8cA7087fA4A2A1FB6357269965A2014ABc35` |
| USDC/USDT | `0x3416cF6C708Da44DB2624D63ea0AAef7113527C6` |

### Why two pools were replaced

The first run returned 0 swaps for XDP/USDC and WBTC/USDC. Instead of accepting the zeros, I checked both addresses with `eth_getCode`, at the end of the block range and at the latest block. Both returned `0x`, meaning there is no contract at those addresses on Ethereum mainnet, so they can never emit Swap events there:

- `0x2dF380544B88AdB3ad0A94100dcC45fd705aAE2d` (XDP/USDC): no contract code on mainnet.
- `0x0E4831319A50228B9e450861297aB92dee15B44F` (WBTC/USDC): this is a WBTC/USDC pool on **Arbitrum**, not Ethereum.

As agreed with the instructor, I replaced them with two live mainnet Uniswap V3 pools: the mainnet WBTC/USDC pool and the USDC/USDT pool. To stop this from failing silently again, the script now checks that every pool has contract code before it fetches anything, and prints a warning if one doesn't.

## Requirements

- Python 3.10+ and [uv](https://docs.astral.sh/uv/)
- Docker (the Postgres database from Assignment 1)
- An Ethereum mainnet RPC endpoint (defaults to `https://eth.drpc.org`)

Dependencies (`web3`, `psycopg`) are declared inline at the top of the script, so `uv run` installs them automatically.

## How to run

**1. Start Postgres** (the container from Assignment 1):

```bash
cd assignment1_jaybeecode
docker compose up -d db
```

**2. Set the connection settings:**

```bash
export DATABASE_URL="postgresql://root:root@localhost:5433/crypto"
export ETH_RPC_URL="https://eth.drpc.org"
```

**3. Run the pipeline** (both block numbers are inclusive):

```bash
cd assignment2_jaybeecode
uv run swap_pipeline.py --from 20000000 --to 20000199
```

Optional flags: `--chunk-size` (default 10), `--rpc-url`, `--db-url`.

## How it works

1. **Pool check.** Calls `eth_getCode` for each pool at the end block and warns if a pool has no contract, so a wrong address shows up immediately instead of as a silent 0.
2. **Chunked fetching.** Splits the range into 10-block chunks so the RPC does not cap or reject the request. Each `eth_getLogs` call covers all three pools at once, because the `address` filter accepts a list.
3. **Swap-only filter.** Filters on `topic0`, the keccak256 hash of the Swap event signature (`0xc42079f9…`), so the RPC returns only swaps and not Mint, Burn or Collect events.
4. **Decoding.** Each raw log is decoded with the Uniswap V3 Swap ABI using `process_log`, which turns the hex `data` and `topics` into readable fields.
5. **Timestamps.** Logs do not include a timestamp, so the script fetches each block once and caches it.
6. **Loading.** Each chunk is inserted and committed, so a crash keeps everything saved up to that point.
7. **Retries.** RPC calls retry with exponential backoff on rate limits or timeouts.

## Table schema

```sql
CREATE TABLE swaps (
    pool_address    TEXT          NOT NULL,
    block_number    BIGINT        NOT NULL,
    tx_hash         TEXT          NOT NULL,
    log_index       INTEGER       NOT NULL,
    sender          TEXT          NOT NULL,
    recipient       TEXT          NOT NULL,
    amount0         NUMERIC(78,0) NOT NULL,
    amount1         NUMERIC(78,0) NOT NULL,
    sqrt_price_x96  NUMERIC(78,0) NOT NULL,
    liquidity       NUMERIC(78,0) NOT NULL,
    tick            INTEGER       NOT NULL,
    timestamp       TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (tx_hash, log_index)
);
```

Design choices:

- **No duplicates.** `(tx_hash, log_index)` uniquely identifies an event log, so it is the primary key. Inserts use `ON CONFLICT DO NOTHING`, which makes re-running any range safe.
- **`NUMERIC(78,0)` for amounts.** `amount0`, `amount1`, `sqrtPriceX96` and `liquidity` are up to 256-bit integers, far too large for `BIGINT`. `NUMERIC(78,0)` stores them exactly.
- **`sqrt_price_x96` column name.** Postgres lowercases unquoted identifiers, so `sqrtPriceX96` is stored as `sqrt_price_x96` to keep queries simple.
- Negative `amount0` or `amount1` values are expected: they mean that token left the pool.

## Results (blocks 20,000,000 to 20,000,199)

| Pool | Swaps |
| --- | --- |
| ETH/USDC | 180 |
| WBTC/USDC | 1 |
| USDC/USDT | 24 |
| **Total** | **205** |

Query used:

```sql
SELECT pool_address, COUNT(*) AS swaps
FROM swaps
GROUP BY pool_address
ORDER BY swaps DESC;
```

## Proof

**1. First run:** swaps fetched, decoded and inserted.

![First run](/assignment2_jaybeecode/screenshots/1-first-run.png)

**2. Re-run of the same range:** every chunk reports `0 new` and the counts are unchanged, so no duplicates were created.

![Re-run, no duplicates](/assignment2_jaybeecode/screenshots/2-rerun-no-duplicates.png)

**3. Row counts per pool in Postgres (DBeaver).**

![DBeaver counts](/assignment2_jaybeecode/screenshots/3-dbeaver-counts.png)

## Troubleshooting

| Problem | Cause | Fix |
| --- | --- | --- |
| `403 Forbidden` from `ethereum-rpc.publicnode.com` | The public RPC blocked script requests | Switched to `https://eth.drpc.org` via `ETH_RPC_URL` |
| `Connection refused` on port 5433 | The Postgres container was not running | Started Docker Desktop, then `docker compose up -d db` |
| 0 swaps for two pools | Addresses had no contract on Ethereum mainnet | Verified with `eth_getCode`, replaced the pools, added an automatic check |