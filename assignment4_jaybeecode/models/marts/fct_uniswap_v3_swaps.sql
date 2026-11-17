-- Mart: one row per Uniswap V3 swap (grain = tx_hash + log_index).
-- The table analysts and BI tools read. Aggregations (daily volume,
-- per-wallet activity, etc.) are built on top of it.

{{ config(materialized='table') }}

select
    tx_hash || '-' || log_index::text   as swap_id,
    tx_hash,
    log_index,
    block_timestamp,
    block_timestamp::date               as swap_date,
    block_number,
    pool_name,
    pool_address,
    fee_tier_bps,
    direction,
    token_in_symbol,
    amount_in,
    token_out_symbol,
    amount_out,
    base_token_symbol,
    base_token_price_usdc,
    volume_usdc,
    trade_size_bucket,
    sender_address,
    recipient_address

from {{ ref('int_uniswap_v3_swaps_enriched') }}
