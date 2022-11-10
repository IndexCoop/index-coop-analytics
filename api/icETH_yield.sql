-- https://dune.com/queries/1354891

with

rates as (
select 
    a.hour,
    a.rate as eth_borrow_rate,
    l.apr as steth_yield,
    (a.rate + 1)^(0.00011415525) as aave_rate, --1/(365*24)
    1 + (l.apr / 365 / 24) as lido_rate
from        (select * from dune_user_generated.aave_weth_borrow_rate_hourly where hour >= '2022-04-01 16:00') a
left join   (
            select
                hour,
                (select pd_apr from dune_user_generated.wsteth_updates t where t.pd_start <= h.hour and pd_apr is not null order by t.pd_start desc limit 1) as apr
            from    (select generate_series('2022-04-01 16:00', date_trunc('hour', now()), '1 hour') as hour) h
            ) l on a.hour = l.hour
),

summary as (
select
    hour,
    iceth_apy,
    leverage_ratio,
    aave_apy,
    lido_apy,
    roi_all_time,
    (1+ roi_all_time - lag(roi_all_time, (24*30)) over (order by hour asc))^(365/30) -1 as "30d APY",
    (1+ roi_all_time - lag(roi_all_time, (24*60)) over (order by hour asc))^(365/60) -1 as "60d APY",
    (1+ roi_all_time - lag(roi_all_time, (24*90)) over (order by hour asc))^(365/90) -1 as "90d APY",
    iceth_apy - (f.streaming_fee) as "Net Yield vs ETH",
    iceth_apy - lido_apy - (f.streaming_fee) as "Net Yield vs stETH",
    iceth_apy as "Gross Yield vs ETH",
    iceth_apy - lido_apy as "Gross Yield vs stETH",
    real_return as "Real Return",
    f.streaming_fee as "Streaming Fee",
    parity_value
from        (
            select
                hour,
                hour_rate ^ (365*24) - 1 as iceth_apy,
                lev_ratio as leverage_ratio,
                aave_rate ^ (365*24) - 1 as aave_apy,
                lido_rate ^ (365*24) - 1 as lido_apy,
                EXP(SUM(LN(hour_rate)) over (order by hour asc rows between unbounded preceding and current row)) - 1  as roi_all_time,
                real_return,
                parity_value
            from    (
                    select
                        i.hour,
                        i.lev_ratio,
                        r.aave_rate,
                        r.lido_rate,
                        i.lev_ratio * r.lido_rate - (i.lev_ratio - 1) * r.aave_rate as hour_rate,
                        parity_value - 1 as real_return,
                        parity_value
                    from        dune_user_generated.iceth_composition i
                    left join   rates r on i.hour = r.hour
                    ) t0
            ) t1
left join   (
            select * from dune_user_generated.indexcoop_fee_structure
            where token_address = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84'
            ) f on date_trunc('day', t1.hour) = f.day
), 

apy_table as (
select 
    s.hour as "Hour",
    s.leverage_ratio as "Leverage Ratio",
    s.aave_apy as "Aave APY",
    s.lido_apy as "Lido stETH APY",
    s.roi_all_time as "Theoretical Return",
    "30d APY",
    "60d APY",
    "90d APY",
    "Net Yield vs ETH",
    "Net Yield vs stETH",
    "Gross Yield vs ETH",
    "Gross Yield vs stETH",
    "Real Return",
    "Streaming Fee",
    case when s.hour <= '2022-09-16' then null else "Net Yield vs ETH" end as "Post-Merge APY",
    text((round("Net Yield vs ETH"::numeric * 100,2))) || '%' as net_yield_eth_txt,
    text((round(s.aave_apy::numeric * 100,2))) || '%' as aave_apy_txt,
    text((round(s.lido_apy::numeric * 100,2))) || '%' as lido_apy_txt,
    text((round("Streaming Fee"::numeric * 100,2))) || '%' as streaming_fee_txt,
    parity_value as "Parity Value ETH",
    parity_value * p.price as "Parity Value USD",
    parity_value * p1.price as "Net Asset Value"
from        summary s
left join   prices.layer1_usd p on p.minute = s.hour and p.symbol = 'ETH'
left join   prices.usd p1 on p1.minute = s.hour and p1.contract_address = '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'
order by 1 desc
)

select * from apy_table 
order by "Hour" desc
limit 1
