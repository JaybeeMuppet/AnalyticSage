-- Staging: clean, cast and rename only. No business logic.

with source as (

    select * from {{ source('uniswap_raw', 'raw_uniswap_v3_swaps') }}

),

renamed as (

    select
        -- natural key
        lower(tx_hash)                    as tx_hash,
        log_index::integer                as log_index,

        -- context
        lower(pool_address)               as pool_address,
        block_number::bigint              as block_number,
        block_timestamp::timestamptz      as block_timestamp,
        lower(sender)                     as sender_address,
        lower(recipient)                  as recipient_address,

        -- raw signed amounts from the pool's point of view
        amount0::numeric(78, 0)           as amount0_raw,
        amount1::numeric(78, 0)           as amount1_raw,

        -- pool state after the swap
        sqrt_price_x96::numeric(78, 0)    as sqrt_price_x96,
        liquidity::numeric(78, 0)         as liquidity,
        tick::integer                     as tick

    from source

)

select * from renamed
