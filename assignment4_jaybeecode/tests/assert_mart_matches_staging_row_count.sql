-- Fails if the mart has a different number of swaps than staging (rows lost or duplicated).
select *
from (select count(*) as n from {{ ref('stg_uniswap_v3_swaps') }}) s
cross join (select count(*) as m from {{ ref('fct_uniswap_v3_swaps') }}) f
where s.n <> f.m
