-- Fails if any raw swap belongs to a pool missing from the seed.
-- The intermediate model inner-joins on pools, so such swaps would silently vanish from the mart.
select s.pool_address, count(*) as swaps
from {{ ref('stg_uniswap_v3_swaps') }} s
left join {{ ref('stg_uniswap_v3_pools') }} p on s.pool_address = p.pool_address
where p.pool_address is null
group by s.pool_address
