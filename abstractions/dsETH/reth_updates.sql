-- https://dune.com/queries/1548562

create or replace view dune_user_generated.reth_updates as 
 -- BalancesUpdated reports a snapshot from a previous block `block`, so we need to get the time at which that
 -- snapshot block # occurred to report the accurate time.
    
select
    pd_start,
    pd_length,
    365 * (rate_increase / pd_length) as pd_apr,
    pd_start_nav
from    (
        SELECT 
            a.time as pd_start,
            extract(epoch from lead(a.time,1) over (order by a.time - a.time) - a.time)/60/60/24 as pd_length,
            b.er as pd_start_nav,
            (lead(b.er,1) over (order by a.time) / b.er) - 1 as rate_increase
        FROM    (
                select
                    time, 
                    "number" as block
                from    ethereum.blocks
                where   "number" > 13578840
                ) a 
        JOIN    (
                select
                    block,
                    "totalEth" / "rethSupply" as er
                from    rocketnetwork."RocketNetworkBalances_evt_BalancesUpdated"
                where   block > 13578840
                ) b 
        ON      a.block = b.block 
        ) t


