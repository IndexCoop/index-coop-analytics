-- https://dune.com/queries/1548421

create or replace view dune_user_generated.steth_daily_apr as
select
    day,
    apr
from    (
        select
            day,
            lead(apr,1) over (order by day) as apr
        from    (
                select 
                    date_trunc('day', evt_block_time) as day,
                    365 * ("postTotalPooledEther" / "totalShares" / (lag("postTotalPooledEther" / "totalShares", 1) over (order by evt_block_time)) - 1 ) AS apr
                from    lido."LidoOracle_evt_PostTotalShares"
                ) t0
        ) t1
where   apr is not null
