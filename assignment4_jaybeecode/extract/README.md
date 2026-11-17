# Extract: Uniswap V3 swaps into Postgres

`load_swaps.py` puts the raw swaps into the `raw_uniswap_v3_swaps` table in Postgres.

This is **my own extractor from Assignment 2** (`assignment2_jaybeecode/swap_pipeline.py`), reworked to read its settings from `.env` and to support a committed snapshot.

## Run it (from the project root)

```bash
uv run extract/load_swaps.py
```

This loads the committed snapshot, `data/uniswap_v3_swaps_snapshot.csv`. It needs no RPC, so it works even if the RPC is down or rate-limited.

## Re-extract fresh data from Ethereum (optional)

```bash
uv run extract/load_swaps.py --source rpc --from-block 20000000 --to-block 20000199
```

Needs `WEB3_RPC_URL` in `.env`. It fetches Swap logs for the three pools in 10-block chunks, decodes them with the Swap ABI (`process_log`), loads them, and merges them into the snapshot CSV.

## Idempotent

The table's primary key is `(tx_hash, log_index)`, which uniquely identifies a swap, and inserts use `ON CONFLICT DO NOTHING`. Running the same input twice inserts 0 new rows. The log line at the end shows how many rows were new and how many were already there.
