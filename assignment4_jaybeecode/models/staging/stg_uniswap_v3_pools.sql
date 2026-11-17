-- Staging: pool metadata from the seed, cast and renamed only.

select
    lower(pool_address)          as pool_address,
    pool_name::text              as pool_name,
    token0_symbol::text          as token0_symbol,
    token0_decimals::integer     as token0_decimals,
    token1_symbol::text          as token1_symbol,
    token1_decimals::integer     as token1_decimals,
    fee_tier_bps::numeric        as fee_tier_bps

from {{ ref('uniswap_v3_pools') }}
