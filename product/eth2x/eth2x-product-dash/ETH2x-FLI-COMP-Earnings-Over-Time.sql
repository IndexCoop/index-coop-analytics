--https://dune.xyz/queries/143124--
with borrow_comp as (
select sum("compDelta"/1e18) as total, date_trunc('day', evt_block_time) as comp_date from compound_v2."Unitroller_evt_DistributedBorrowerComp"
where borrower = '\xAa6E8127831c9DE45ae56bB1b0d4D4Da6e5665BD'
group by comp_date),
supply_comp as (select sum("compDelta"/1e18) as total, date_trunc('day', evt_block_time) as comp_date from compound_v2."Unitroller_evt_DistributedSupplierComp"
where supplier = '\xAa6E8127831c9DE45ae56bB1b0d4D4Da6e5665BD'
group by comp_date),
agg as (select * from borrow_comp
union all 
select * from supply_comp), 
comp_prices as (select date_trunc('day', minute) as price_day, avg(price) as comp_price from prices."usd"
where symbol = 'COMP'
and minute > now() - interval '12 months'
group by price_day
order by price_day), 
comp_daywise as (select sum(total) as total_comp_earned, comp_date from agg
group by comp_date),
pretty as (
select total_comp_earned, 
comp_date, 
comp_price, 
price_day
from comp_daywise
inner join comp_prices on price_day = comp_date
)
select comp_date, 
sum(total_comp_earned * comp_price) over (order by comp_date)
as comp_USD_earnings 
from pretty
