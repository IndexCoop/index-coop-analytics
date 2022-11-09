-- https://dune.com/queries/1554246

create or replace view dune_user_generated.cbeth_updates as 

select
    pd_start,
    pd_length,
    365 * (rate_increase / pd_length) as pd_apr,
    pd_start_nav
from    (
        select
            time as pd_start,
            lead(time,1) over (order by time) as pd_end,
            extract(epoch from lead(time,1) over (order by time) - time)/60/60/24 as pd_length,
            (lead(er,1) over (order by time) / er) - 1 as rate_increase,
            er/1e18 as pd_start_nav,
            (lead(er,1) over (order by time))/1e18 as pd_end_nav
        from    (
                select 
                    "evt_block_time" as time,
                    "newExchangeRate" as er
                from coinbase."StakedTokenV1_evt_ExchangeRateUpdated"
                ) t0
        ) t1

