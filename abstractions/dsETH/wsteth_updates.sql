-- https://dune.com/queries/1548421

create or replace view dune_user_generated.wsteth_updates as

select
    pd_start,
    pd_length,
    365 * (rate_increase / pd_length) as pd_apr,
    pd_start_nav
from    (
        select
            time as pd_start,
            extract(epoch from lead(time,1) over (order by time) - time)/60/60/24 as pd_length,
            (lead(er,1) over (order by time) / er) - 1 as rate_increase,
            er as pd_start_nav
        from    (
                select
                    "evt_block_time" as time,
                    "postTotalPooledEther" / "totalShares" as er
                from    lido."LidoOracle_evt_PostTotalShares"
                ) t0
        ) t1
