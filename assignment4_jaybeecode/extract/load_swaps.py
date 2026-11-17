"""
Load Uniswap V3 swaps into Postgres (raw layer for the dbt project).

This is my Week 2 swap extractor (assignment2_jaybeecode/swap_pipeline.py),
reworked to support a committed snapshot and to read settings from .env.

Two sources:

  snapshot (default)  Load data/uniswap_v3_swaps_snapshot.csv into Postgres.
                      Needs no RPC, so the pipeline always runs.

  rpc                 Fetch Swap logs from Ethereum over RPC for a block range,
                      load them, and merge them into the snapshot CSV.

Idempotent: the raw table's primary key is (tx_hash, log_index) and inserts use
ON CONFLICT DO NOTHING, so loading the same data twice adds 0 rows.

Usage (from the project root):
    uv run extract/load_swaps.py
    uv run extract/load_swaps.py --source rpc --from-block 20000000 --to-block 20000199
"""

import argparse
import csv
import logging
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT_PATH = PROJECT_ROOT / "data" / "uniswap_v3_swaps_snapshot.csv"

POOLS = {
    "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640": "ETH/USDC",
    "0x99ac8ca7087fa4a2a1fb6357269965a2014abc35": "WBTC/USDC",
    "0x3416cf6c708da44db2624d63ea0aaef7113527c6": "USDC/USDT",
}

COLUMNS = [
    "pool_address", "block_number", "block_timestamp", "tx_hash", "log_index",
    "sender", "recipient", "amount0", "amount1", "sqrt_price_x96", "liquidity", "tick",
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw_uniswap_v3_swaps (
    pool_address    TEXT          NOT NULL,
    block_number    BIGINT        NOT NULL,
    block_timestamp TIMESTAMPTZ   NOT NULL,
    tx_hash         TEXT          NOT NULL,
    log_index       INTEGER       NOT NULL,
    sender          TEXT          NOT NULL,
    recipient       TEXT          NOT NULL,
    amount0         NUMERIC(78,0) NOT NULL,
    amount1         NUMERIC(78,0) NOT NULL,
    sqrt_price_x96  NUMERIC(78,0) NOT NULL,
    liquidity       NUMERIC(78,0) NOT NULL,
    tick            INTEGER       NOT NULL,
    loaded_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (tx_hash, log_index)
);
"""

INSERT_SQL = f"""
INSERT INTO raw_uniswap_v3_swaps ({", ".join(COLUMNS)})
VALUES ({", ".join("%(" + c + ")s" for c in COLUMNS)})
ON CONFLICT (tx_hash, log_index) DO NOTHING;
"""

SWAP_EVENT_ABI = [{
    "anonymous": False, "name": "Swap", "type": "event",
    "inputs": [
        {"indexed": True, "name": "sender", "type": "address"},
        {"indexed": True, "name": "recipient", "type": "address"},
        {"indexed": False, "name": "amount0", "type": "int256"},
        {"indexed": False, "name": "amount1", "type": "int256"},
        {"indexed": False, "name": "sqrtPriceX96", "type": "uint160"},
        {"indexed": False, "name": "liquidity", "type": "uint128"},
        {"indexed": False, "name": "tick", "type": "int24"},
    ],
}]

log = logging.getLogger("load_swaps")


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

def db_conninfo():
    """Build the Postgres connection string from DB_* variables (no hardcoded secrets)."""
    missing = [v for v in ("DB_USER", "DB_PASS", "DB_NAME") if not os.getenv(v)]
    if missing:
        sys.exit(f"Missing {', '.join(missing)}. Copy .env.example to .env and fill it in.")
    return psycopg.conninfo.make_conninfo(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        dbname=os.environ["DB_NAME"],
    )


# ---------------------------------------------------------------------------
# Snapshot CSV
# ---------------------------------------------------------------------------

def read_snapshot(path):
    """Read the snapshot CSV into rows shaped for INSERT_SQL."""
    if not path.exists():
        sys.exit(f"Snapshot not found: {path}. Run with --source rpc first to create it.")
    rows = []
    with path.open(newline="") as f:
        for r in csv.DictReader(f):
            rows.append({
                "pool_address": r["pool_address"].lower(),
                "block_number": int(r["block_number"]),
                "block_timestamp": datetime.fromisoformat(r["block_timestamp"]),
                "tx_hash": r["tx_hash"].lower(),
                "log_index": int(r["log_index"]),
                "sender": r["sender"].lower(),
                "recipient": r["recipient"].lower(),
                "amount0": Decimal(r["amount0"]),
                "amount1": Decimal(r["amount1"]),
                "sqrt_price_x96": Decimal(r["sqrt_price_x96"]),
                "liquidity": Decimal(r["liquidity"]),
                "tick": int(r["tick"]),
            })
    return rows


def write_snapshot(path, new_rows):
    """Merge new rows into the snapshot, keyed on (tx_hash, log_index), sorted by block."""
    merged = {}
    if path.exists():
        for r in read_snapshot(path):
            merged[(r["tx_hash"], r["log_index"])] = r
    for r in new_rows:
        merged[(r["tx_hash"], r["log_index"])] = r
    ordered = sorted(merged.values(), key=lambda r: (r["block_number"], r["log_index"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        for r in ordered:
            writer.writerow({**r, "block_timestamp": r["block_timestamp"].isoformat()})
    log.info("Snapshot %s now holds %d swaps", path.relative_to(PROJECT_ROOT), len(ordered))


# ---------------------------------------------------------------------------
# RPC extraction (from the Week 2 pipeline)
# ---------------------------------------------------------------------------

def with_retries(fn, *args, attempts=5, base_delay=1.0, **kwargs):
    for attempt in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            if attempt == attempts:
                raise
            delay = base_delay * 2 ** (attempt - 1)
            log.warning("Attempt %d/%d failed (%s). Retrying in %.0fs", attempt, attempts, exc, delay)
            time.sleep(delay)


def chunk_ranges(start, end, size):
    """Split the inclusive range [start, end] into inclusive chunks."""
    for chunk_start in range(start, end + 1, size):
        yield chunk_start, min(chunk_start + size - 1, end)


def fetch_from_rpc(rpc_url, from_block, to_block, chunk_size):
    from web3 import Web3  # only needed for this source

    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))
    if not w3.is_connected():
        sys.exit(f"Could not connect to RPC at {rpc_url}")
    if w3.eth.chain_id != 1:
        sys.exit(f"RPC is on chain {w3.eth.chain_id}, expected Ethereum mainnet (1)")

    addresses = [Web3.to_checksum_address(a) for a in POOLS]
    for address in addresses:
        if len(with_retries(w3.eth.get_code, address, to_block)) == 0:
            log.warning("%s has no contract code at block %d: expect 0 swaps for it.", address, to_block)

    swap_event = w3.eth.contract(abi=SWAP_EVENT_ABI).events.Swap()
    topic = Web3.to_hex(Web3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)"))
    timestamps = {}
    rows = []
    chunks = list(chunk_ranges(from_block, to_block, chunk_size))
    for i, (start, end) in enumerate(chunks, 1):
        raw_logs = with_retries(w3.eth.get_logs, {
            "fromBlock": start, "toBlock": end, "address": addresses, "topics": [topic],
        })
        for raw in raw_logs:
            event = swap_event.process_log(raw)
            a = event["args"]
            block = event["blockNumber"]
            if block not in timestamps:
                ts = with_retries(w3.eth.get_block, block)["timestamp"]
                timestamps[block] = datetime.fromtimestamp(ts, tz=timezone.utc)
            rows.append({
                "pool_address": event["address"].lower(),
                "block_number": block,
                "block_timestamp": timestamps[block],
                "tx_hash": Web3.to_hex(event["transactionHash"]).lower(),
                "log_index": event["logIndex"],
                "sender": a["sender"].lower(),
                "recipient": a["recipient"].lower(),
                "amount0": Decimal(a["amount0"]),
                "amount1": Decimal(a["amount1"]),
                "sqrt_price_x96": Decimal(a["sqrtPriceX96"]),
                "liquidity": Decimal(a["liquidity"]),
                "tick": a["tick"],
            })
        log.info("Chunk %d/%d | blocks %d-%d | %d swaps", i, len(chunks), start, end, len(raw_logs))
    return rows


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def load(rows):
    with psycopg.connect(db_conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            inserted = 0
            for row in rows:
                cur.execute(INSERT_SQL, row)
                inserted += cur.rowcount
        conn.commit()
        total = conn.execute("SELECT COUNT(*) FROM raw_uniswap_v3_swaps").fetchone()[0]
    log.info("%d swaps in input, %d new rows inserted, %d already present. Table now has %d rows.",
             len(rows), inserted, len(rows) - inserted, total)


def main():
    load_dotenv(PROJECT_ROOT / ".env")
    parser = argparse.ArgumentParser(description="Load Uniswap V3 swaps into Postgres.")
    parser.add_argument("--source", choices=["snapshot", "rpc"], default="snapshot")
    parser.add_argument("--from-block", type=int, help="first block, inclusive (rpc only)")
    parser.add_argument("--to-block", type=int, help="last block, inclusive (rpc only)")
    parser.add_argument("--chunk-size", type=int, default=10, help="blocks per eth_getLogs request")
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT_PATH)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if args.source == "snapshot":
        rows = read_snapshot(args.snapshot)
        log.info("Read %d swaps from %s", len(rows), args.snapshot.name)
    else:
        if args.from_block is None or args.to_block is None or args.to_block < args.from_block:
            parser.error("--source rpc needs --from-block and --to-block (to >= from)")
        rpc_url = os.getenv("WEB3_RPC_URL")
        if not rpc_url:
            sys.exit("WEB3_RPC_URL is not set in .env")
        rows = fetch_from_rpc(rpc_url, args.from_block, args.to_block, args.chunk_size)
        write_snapshot(args.snapshot, rows)

    load(rows)


if __name__ == "__main__":
    main()
