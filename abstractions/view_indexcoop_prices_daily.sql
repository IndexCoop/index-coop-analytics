-- Ethereum - 

CREATE OR REPLACE VIEW 
dune_user_generated.indexcoop_prices_daily as

(
with

eligible_pairs as (
      select 'WETH' as symbol
union select 'WMATIC'
union select 'USDC'
),

trades as (
select
    date_trunc('minute', a.block_time) as minute,
    b.symbol as index_symbol,
    a.token_b_symbol as base_symbol,
    a.token_b_amount / nullif((a.token_a_amount_raw/1e18),0) as relative_price
from        dex."trades" a
inner join  dune_user_generated."indexcoop_tokens" b on a.token_a_address = b.token_address
inner join  eligible_pairs ep on a.token_b_symbol = ep.symbol
where a.block_time > '2022-03-13' -- this is the last cache date

union all

select
    date_trunc('minute', a.block_time) as minute,
    b.symbol as index_symbol,
    a.token_a_symbol as base_symbol,
    a.token_a_amount / nullif((a.token_b_amount_raw/1e18),0) as relative_price
from        dex."trades" a
inner join  dune_user_generated."indexcoop_tokens" b on a.token_b_address = b.token_address
inner join  eligible_pairs ep on a.token_a_symbol = ep.symbol
where a.block_time > '2022-03-13'
),

base_prices as (
select
    p.symbol,
    p.minute,
    p.price
from        prices.usd p
inner join  eligible_pairs ep on p.symbol = ep.symbol
where       minute >= '2022-03-13' -- Cache date
),

usd_trades as (
select
    a.minute,
    a.index_symbol as symbol,
    a.relative_price * b.price as index_usd
from        trades a
left join   base_prices b on a.minute = b.minute and a.base_symbol = b.symbol
),

price_feed as (
select
    symbol,
    date_trunc('day', minute) as day,
    percentile_cont(.5) within group (order by index_usd) as price -- faster to get the VWAP
from        usd_trades
group by    day, symbol
order by    symbol, day
)

select * from price_feed
union all
select * from dune_user_generated.indexcoop_prices_daily_cached
)