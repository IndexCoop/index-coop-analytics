-- https://dune.com/queries/1548562

create or replace view dune_user_generated.reth_daily_apr as 
(
with

blocks as ( 
select 
    time,
    extract(epoch from time) as epoch, 
    "number" as block
from    ethereum.blocks
where   "number" > 13578840
),
 -- BalancesUpdated reports a snapshot from a previous block `block`, so we need to get the time at which that
 -- snapshot block # occurred to report the accurate time.
balances_updated as (
select
    block,
    "totalEth" / "rethSupply" as eth_reth_ratio
from    rocketnetwork."RocketNetworkBalances_evt_BalancesUpdated"
where   block > 13578840
)
    
select
    date_trunc('day', time) as day,
    365*avg(ratio_increase/time_increase) as apr
from    (
        SELECT 
            time,
            b.block,
            eth_reth_ratio,
            (eth_reth_ratio / lag(eth_reth_ratio,1) over (order by b.block)- 1) as ratio_increase,
            extract(epoch from time - lag(time,1) over (order by b.block))/60/60/24 as time_increase
        FROM blocks b 
        JOIN balances_updated d 
        ON b.block = d.block 
        ) t
group by 1
)

