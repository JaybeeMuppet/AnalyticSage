-- Intermediate: the business decisions.
--   * join each swap to its pool's tokens and decimals
--   * work out trade direction from the pool's signed amounts
--   * convert raw amounts to human units
--   * decode sqrtPriceX96 into a readable price
--   * value each trade in USDC (every pool here has a USDC leg)
--   * bucket trades by size

with swaps as (

    select * from {{ ref('stg_uniswap_v3_swaps') }}

),

pools as (

    select * from {{ ref('stg_uniswap_v3_pools') }}

),

joined as (

    select
        swaps.*,
        pools.pool_name,
        pools.fee_tier_bps,
        pools.token0_symbol,
        pools.token1_symbol,
        pools.token0_decimals,
        pools.token1_decimals,

        -- raw -> human units
        swaps.amount0_raw / power(10::numeric, pools.token0_decimals) as amount0,
        swaps.amount1_raw / power(10::numeric, pools.token1_decimals) as amount1,

        -- sqrtPriceX96 -> token1 per token0, adjusted for decimals.
        -- ::numeric keeps the maths exact instead of floating point.
        power(swaps.sqrt_price_x96 / power(2::numeric, 96), 2)
            * power(10::numeric, pools.token0_decimals - pools.token1_decimals)
            as price_token1_per_token0

    from swaps
    inner join pools
        on swaps.pool_address = pools.pool_address

),

decided as (

    select
        *,

        -- Positive amount = tokens went INTO the pool, i.e. the trader sold them.
        case when amount0_raw > 0 then 'token0_to_token1'
             else 'token1_to_token0' end                         as direction,

        case when amount0_raw > 0 then token0_symbol
             else token1_symbol end                              as token_in_symbol,
        case when amount0_raw > 0 then token1_symbol
             else token0_symbol end                              as token_out_symbol,
        case when amount0_raw > 0 then amount0 else amount1 end  as amount_in,
        case when amount0_raw > 0 then -amount1 else -amount0 end as amount_out,

        -- The non-USDC token is the one being priced.
        case when token0_symbol = 'USDC' then token1_symbol
             else token0_symbol end                              as base_token_symbol,
        case when token0_symbol = 'USDC' then 1 / price_token1_per_token0
             else price_token1_per_token0 end                    as base_token_price_usdc,

        -- Trade value = size of the USDC leg.
        case when token0_symbol = 'USDC' then abs(amount0)
             else abs(amount1) end                               as volume_usdc

    from joined

)

select
    tx_hash,
    log_index,
    pool_address,
    pool_name,
    fee_tier_bps,
    block_number,
    block_timestamp,
    sender_address,
    recipient_address,
    direction,
    token_in_symbol,
    token_out_symbol,
    round(amount_in, 8)                  as amount_in,
    round(amount_out, 8)                 as amount_out,
    base_token_symbol,
    round(base_token_price_usdc, 8)      as base_token_price_usdc,
    round(volume_usdc, 6)                as volume_usdc,
    case
        when volume_usdc < 1000   then 'small'
        when volume_usdc < 10000  then 'medium'
        when volume_usdc < 100000 then 'large'
        else 'whale'
    end                                  as trade_size_bucket,
    liquidity,
    tick

from decided
