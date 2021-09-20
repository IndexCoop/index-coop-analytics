with uniswapv2trades as 
(select "to" as users, 
count(contract_address) as trades 
from uniswap_v2."Pair_evt_Swap"
where contract_address = '\xF91c12DAe1313d0bE5d7A27aa559B1171cC1EaC5'
group by "to"
),
uniswapv3trades as 
(select "from" as users,
count("to") as trades 
from ethereum."transactions"
where "hash" in (
select call_tx_hash 
from uniswap_v3."Pair_call_swap"
where contract_address = '\x151CcB92bc1eD5c6D0F9Adb5ceC4763cEb66AC7f')
group by 1
),
balancertrades as (
select "caller" as users, 
count(contract_address) as trades
from balancer."BPool_evt_LOG_SWAP"
where contract_address = '\xE94A3e724481633974B2722e2BAF275e01a49098'
group by users
),
unitrades as (
select * from uniswapv2trades
union all 
select * from uniswapv3trades
union all 
select * from balancertrades),
tradeagg as (select users, sum(trades) as totaltrades from unitrades
group by users), 
zerotofive as 
(select 
count(users) as users
from tradeagg
where totaltrades < 5
), 
fivetoten as 
(select 
count(users) users
from tradeagg
where totaltrades>5
and totaltrades<10),
tentotwentyfive as (
select 
count(users) users
from tradeagg
where totaltrades>10
and totaltrades<25
),
twentyfivetofifty as (
select 
count(users) users
from tradeagg
where totaltrades>25
and totaltrades<50
),
fiftytohundred as (
select 
count(users) users
from tradeagg
where totaltrades>50
and totaltrades<100
),
hundredplus as (
select 
count(users) users
from tradeagg
where totaltrades>50
and totaltrades<100
),
combined as (
select 
users,
'A(0-5)' as tradefrequency
from zerotofive
union all 
select 
users,
'B(5-10)' as tradefrequency
from fivetoten
union all 
select 
users,
'C(10-25)' as tradefrequency
from tentotwentyfive
union all 
select 
users,
'D(25-50)' as tradefrequency
from twentyfivetofifty
union all 
select 
users,
'E(50-100)' as tradefrequency
from fiftytohundred
union all 
select 
users,
'F(100+)' as tradefrequency
from hundredplus

)
select * from combined
order by users asc
