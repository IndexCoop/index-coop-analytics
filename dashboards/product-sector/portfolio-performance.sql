-- https://dune.com/queries/1067606

with

index_price as (
select
    day,
    price
from    (
        select
            date_trunc('day', hour) as day,
            median_price as price,
            row_number() over (partition by date_trunc('day', hour) order by hour desc) as rnb
        from    prices.prices_from_dex_data
        where   contract_address = (select token_address from dune_user_generated."indexcoop_tokens" where symbol = '{{Index Coop Sector Token:}}')
        and     date_trunc('day', hour) > date_trunc('day', least('{{End Date:}}', now())) - interval '{{Trailing Days:}} days'
        and     date_trunc('day', hour) <= date_trunc('day', least('{{End Date:}}', now()))
        ) t
where       t.rnb = 1
),

-- get the initial price at the start of the period
init_price as (
select 
    * 
from        index_price 
order by    day asc
limit       1
),

-- get the performance of a hypothetical $10,000 in nominal and percentage terms
performance as (
select
    day,
    price / (select price from init_price) * 10000 as value,
    (price / (select price from init_price) * 10000) - 10000 as pnl,
    price / (select price from init_price) - 1 as pct
from    index_price
),

up as (select *, 'Profit' as status from performance where pnl >= 0),
down as (select *, 'Loss' as status from performance where pnl < 0)

select * from up
union
select * from down
