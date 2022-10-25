-- https://dune.xyz/queries/503150

-- drop table if exists dune_user_generated.indexcoop_prices_daily_cached cascade;

CREATE table 
dune_user_generated.indexcoop_prices_daily_cached as

select
    symbol,
    day,
    round(price,2) as price
from    (
        select
            date_trunc('day', p.hour) as day,
            t.symbol,
            p.median_price as price,
            row_number() over (partition by t.symbol, date_trunc('day', p.hour) order by p.hour desc) as rnb
        from        prices.prices_from_dex_data p
        inner join  dune_user_generated."indexcoop_tokens" t on t.token_address = p.contract_address
        where       p.hour < '2022-10-01'
        ) t0
where       t0.rnb = 1
order by    symbol, day
