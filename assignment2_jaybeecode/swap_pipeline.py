# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "web3>=7,<8",
#     "psycopg[binary]>=3.1",
# ]
# ///
"""
Week 2 assignment: extract Uniswap V3 Swap events for three pools into Postgres.

Usage:
    uv run swap_pipeline.py --from 20000000 --to 20000199

Both --from and --to are inclusive. Re-running the same range is safe:
rows are keyed on (tx_hash, log_index), so duplicates are skipped.

Settings (command-line flag, or environment variable):
    --rpc-url / ETH_RPC_URL        Ethereum mainnet RPC endpoint
    --db-url  / DATABASE_URL       Postgres connection string
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

import psycopg
from web3 import Web3

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# The two original pool addresses in the assignment have no contract code on
# Ethereum mainnet (0x0E48...B44F is a WBTC/USDC pool on Arbitrum), so they
# were replaced with live Uniswap V3 mainnet pools, as the instructor allowed.
POOLS = {
    Web3.to_checksum_address("0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"): "ETH/USDC",
    Web3.to_checksum_address("0x99ac8cA7087fA4A2A1FB6357269965A2014ABc35"): "WBTC/USDC",
    Web3.to_checksum_address("0x3416cF6C708Da44DB2624D63ea0AAef7113527C6"): "USDC/USDT",
}

DEFAULT_RPC_URL = "https://eth.drpc.org"
DEFAULT_DB_URL = "postgresql://postgres:postgres@localhost:5432/postgres"

# Uniswap V3 pool Swap event. Only this one event is needed, not the full ABI.
SWAP_EVENT_ABI = [
    {
        "anonymous": False,
        "name": "Swap",
        "type": "event",
        "inputs": [
            {"indexed": True, "name": "sender", "type": "address"},
            {"indexed": True, "name": "recipient", "type": "address"},
            {"indexed": False, "name": "amount0", "type": "int256"},
            {"indexed": False, "name": "amount1", "type": "int256"},
            {"indexed": False, "name": "sqrtPriceX96", "type": "uint160"},
            {"indexed": False, "name": "liquidity", "type": "uint128"},
            {"indexed": False, "name": "tick", "type": "int24"},
        ],
    }
]

# topic0 = keccak256 of the event signature. Logs are filtered on this so the
# RPC only returns Swap events, not Mint/Burn/Collect from the same pools.
SWAP_TOPIC = Web3.to_hex(
    Web3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)")
)

# amount0/amount1 are int256 and sqrtPriceX96/liquidity are uint128+, which are
# far too big for BIGINT. NUMERIC(78,0) holds any 256-bit integer exactly.
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS swaps (
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
CREATE INDEX IF NOT EXISTS swaps_pool_block_idx ON swaps (pool_address, block_number);
"""

INSERT_SQL = """
INSERT INTO swaps (
    pool_address, block_number, tx_hash, log_index, sender, recipient,
    amount0, amount1, sqrt_price_x96, liquidity, tick, timestamp
) VALUES (
    %(pool_address)s, %(block_number)s, %(tx_hash)s, %(log_index)s,
    %(sender)s, %(recipient)s, %(amount0)s, %(amount1)s,
    %(sqrt_price_x96)s, %(liquidity)s, %(tick)s, %(timestamp)s
)
ON CONFLICT (tx_hash, log_index) DO NOTHING;
"""

log = logging.getLogger("swap_pipeline")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def with_retries(fn, *args, attempts=5, base_delay=1.0, **kwargs):
    """Call fn, retrying with exponential backoff on any error (rate limits, timeouts)."""
    for attempt in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # RPC providers raise many different error types
            if attempt == attempts:
                raise
            delay = base_delay * 2 ** (attempt - 1)
            log.warning("Attempt %d/%d failed (%s). Retrying in %.0fs", attempt, attempts, exc, delay)
            time.sleep(delay)


def chunk_ranges(start, end, size):
    """Split the inclusive range [start, end] into inclusive chunks of `size` blocks."""
    for chunk_start in range(start, end + 1, size):
        yield chunk_start, min(chunk_start + size - 1, end)


def check_pools_deployed(w3, block):
    """Warn about any pool with no contract code at `block`.

    A wrong address, or one from another chain, silently returns zero logs.
    This check turns that silent zero into a clear warning.
    """
    missing = []
    for address, name in POOLS.items():
        code = with_retries(w3.eth.get_code, address, block)
        if len(code) == 0:
            missing.append(name)
            log.warning(
                "%s (%s) has no contract code at block %d. Wrong address, wrong chain, "
                "or not deployed yet: expect 0 swaps for it.", name, address, block,
            )
    return missing


def decode_swap(swap_event, raw_log, timestamp):
    """Decode one raw log with the Swap ABI (process_log) and shape it as a table row."""
    event = swap_event.process_log(raw_log)
    args = event["args"]
    return {
        "pool_address": Web3.to_checksum_address(event["address"]),
        "block_number": event["blockNumber"],
        "tx_hash": Web3.to_hex(event["transactionHash"]),
        "log_index": event["logIndex"],
        "sender": args["sender"],
        "recipient": args["recipient"],
        # Decimal keeps these as exact NUMERIC in Postgres, whatever their size.
        "amount0": Decimal(args["amount0"]),
        "amount1": Decimal(args["amount1"]),
        "sqrt_price_x96": Decimal(args["sqrtPriceX96"]),
        "liquidity": Decimal(args["liquidity"]),
        "tick": args["tick"],
        "timestamp": timestamp,
    }


class BlockTimestamps:
    """Looks up block timestamps, caching each block so it is only fetched once."""

    def __init__(self, w3):
        self.w3 = w3
        self.cache = {}

    def get(self, block_number):
        if block_number not in self.cache:
            block = with_retries(self.w3.eth.get_block, block_number)
            self.cache[block_number] = datetime.fromtimestamp(block["timestamp"], tz=timezone.utc)
        return self.cache[block_number]


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run(w3, conn, from_block, to_block, chunk_size):
    swap_event = w3.eth.contract(abi=SWAP_EVENT_ABI).events.Swap()
    timestamps = BlockTimestamps(w3)
    pool_addresses = list(POOLS)

    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()

    chunks = list(chunk_ranges(from_block, to_block, chunk_size))
    total_seen = total_inserted = 0

    for i, (start, end) in enumerate(chunks, 1):
        # One request covers all three pools: `address` accepts a list.
        raw_logs = with_retries(
            w3.eth.get_logs,
            {
                "fromBlock": start,
                "toBlock": end,
                "address": pool_addresses,
                "topics": [SWAP_TOPIC],
            },
        )

        rows = [
            decode_swap(swap_event, raw_log, timestamps.get(raw_log["blockNumber"]))
            for raw_log in raw_logs
        ]

        inserted = 0
        if rows:
            with conn.cursor() as cur:
                for row in rows:
                    cur.execute(INSERT_SQL, row)
                    inserted += cur.rowcount  # 0 when ON CONFLICT skipped the row
        # Commit per chunk, so a crash keeps everything saved up to that point.
        conn.commit()

        total_seen += len(rows)
        total_inserted += inserted
        log.info(
            "Chunk %d/%d | blocks %d-%d | %d swaps found, %d new",
            i, len(chunks), start, end, len(rows), inserted,
        )

    log.info(
        "Done. %d swaps found, %d inserted, %d already in the table.",
        total_seen, total_inserted, total_seen - total_inserted,
    )


def print_counts(conn, from_block, to_block):
    """Print row counts per pool, for this range and for the whole table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pool_address,
                   COUNT(*) FILTER (WHERE block_number BETWEEN %s AND %s) AS in_range,
                   COUNT(*) AS all_time
            FROM swaps
            GROUP BY pool_address
            """,
            (from_block, to_block),
        )
        counts = {pool: (in_range, all_time) for pool, in_range, all_time in cur.fetchall()}

    print(f"\nRow counts per pool (blocks {from_block}-{to_block}):")
    print(f"  {'Pool':<11} {'Address':<44} {'In range':>9} {'All time':>9}")
    for address, name in POOLS.items():
        in_range, all_time = counts.get(address, (0, 0))
        print(f"  {name:<11} {address:<44} {in_range:>9} {all_time:>9}")


def main():
    parser = argparse.ArgumentParser(description="Extract Uniswap V3 swaps into Postgres.")
    parser.add_argument("--from", dest="from_block", type=int, required=True, help="first block (inclusive)")
    parser.add_argument("--to", dest="to_block", type=int, required=True, help="last block (inclusive)")
    parser.add_argument("--chunk-size", type=int, default=10, help="blocks per eth_getLogs request (default 10)")
    parser.add_argument("--rpc-url", default=os.getenv("ETH_RPC_URL", DEFAULT_RPC_URL))
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL", DEFAULT_DB_URL))
    args = parser.parse_args()

    if args.from_block < 0 or args.to_block < args.from_block:
        parser.error("--to must be greater than or equal to --from, and both must be non-negative")
    if args.chunk_size < 1:
        parser.error("--chunk-size must be at least 1")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    w3 = Web3(Web3.HTTPProvider(args.rpc_url, request_kwargs={"timeout": 30}))
    if not w3.is_connected():
        sys.exit(f"Could not connect to RPC at {args.rpc_url}")
    if w3.eth.chain_id != 1:
        sys.exit(f"RPC is on chain {w3.eth.chain_id}, expected Ethereum mainnet (1)")

    check_pools_deployed(w3, args.to_block)

    with psycopg.connect(args.db_url) as conn:
        run(w3, conn, args.from_block, args.to_block, args.chunk_size)
        print_counts(conn, args.from_block, args.to_block)


if __name__ == "__main__":
    main()