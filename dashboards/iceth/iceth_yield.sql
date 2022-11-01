-- https://dune.com/queries/547552

/*

Aave Borrow Rate (APY): Calculated as the average variable borrow rate APY for WETH for each hour. variableBorrowRate is the Theoretical APY
determined by Aave's interest rate model. See: https://docs.aave.com/risk/v/aave-v2/liquidity-risk/borrow-interest-rate

Lido stETH APY: Calculated as the actual return of each day annualized by exponentiating to 365. Lido's oracle posts the Total Pooled Ether and Total Shares.
The current day (pooledEther / totalShares) = currentPooledEtherPerShare
The previous day (pooledEther / totalShares) = previousPooledEtherPerShare
(currentPooledEtherPerShare / previousPooledEtherPerShare) = dayPooledEtherPerShare
dayPooledEtherPerShare ^ 365 = Lido stETH APY based on the daily oracle post

Next, we calculate the hourly interest rates for both Aave and Lido.

(aave_apy + 1)^(0.00011415525) = aave_rate
(lido_apy + 1)^(0.00011415525) = lido_rate

where 0.00011415525 = 1/(365*24)

Next, we use issuance and redemption events of icETH to calculate aSTETH and variableDebtWETH per icETH. Unfortunately, this method is not exactly accurate because
it is reliant on issue and redemption events to take place. In practice, this method is fine because issue and redemption events happen pretty frequently. The slight
differences can occur from the rebasing of aSTETH and variableDebtWETH. We use this method though, because without the ability to call read only functions of the icETH
contract, the only other viable method (as far as I know of) is to use erc20.erc20_evt_transfer, but it turns out that this method is actually less accurate because it
capture any of the rebases. Also, view_token_balances hourly is too slow.

From the above, we get the aSTETH per icETH and the variableDebtWETH per icETH

aSTETH / (aSTETH - variableDebtWETH) =  Parity Leverage Ratio. This is not the true leverage ratio because the market price of stETH needn't be equal to price of WETH,
but the parity leverage ratio will be helpful for calculating APY which should be calculated based on parity.

aSTETH - variableDebtWETH = Parity Value. This is simply the collateral - debt. Assuming stETH = ETH, the Parity Value is the value of icETH in terms of ETH.

Then, we get the daily fees charged to icETH from Index Coop using an abstraction based of Set Protocol fee changes.

The hourly performance of icETH (assuming parity) = (stETH hourly performance * amount of stETH) - (aave hourly borrow rate * amount of variableDebtWETH) OR put another way:

leverageRatio * lidoHourRate - (leverageRatio - 1) * aaveHourRate

then ... (icethHourPerformance)^(365*24) - 1 = icethGrossAPY

I opted not to include the Index Coop fees in the prior step of calculating the hourly performance because although Index Coop could claim fees each hour in principle,
in practice fees are usually accrued monthly.

icethNetAPY = icethGrossAPY - icethStreamingFee
*/


with

aave_borrow_rates as (
select
    gs.hour,
    rate
from    (
        select
            hour,
            lead(hour, 1,date_trunc('hour', now() + '1 hour'::interval) ) over (order by hour asc) as next_hour,
            rate
        from    (
                select  
                    date_trunc('hour', a."evt_block_time") as hour,
                    avg(a."variableBorrowRate")/1e27 as rate
                from        aave_v2."LendingPool_evt_ReserveDataUpdated" a
                where       a.reserve = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                and         a."evt_block_time" >= '2022-03-20'
                group by    1
                ) ta
        ) t
inner join  (select generate_series('2022-04-01 16:00', date_trunc('hour', now()), '1 hour') as hour) gs on t.hour <= gs.hour and gs.hour < t.next_hour
),

temp_lido_apy as (
select 
    date_trunc('hour', evt_block_time) as hour,
    (("postTotalPooledEther" / "totalShares" / (lag("postTotalPooledEther" / "totalShares", 1) over (order by evt_block_time)))^365 - 1) AS apy
from    lido."LidoOracle_evt_PostTotalShares"
),

lido_apy as (
select
    hour,
    (select apy from temp_lido_apy t where t.hour <= h.hour order by t.hour desc limit 1) as apy
from    (select generate_series('2022-04-01 16:00', date_trunc('hour', now()), '1 hour') as hour) h
),

rates as (
select 
    a.hour,
    a.rate as eth_borrow_rate,
    l.apy as steth_yield,
    (a.rate + 1)^(0.00011415525) as aave_rate, --1/(365*24)
    (l.apy + 1)^(0.00011415525) as lido_rate
from        aave_borrow_rates a
left join   lido_apy l on a.hour = l.hour
),

issue_events as (
select
    i."evt_block_time",
    i."evt_tx_hash",
    sum(e."value"/1e18) filter (where e."contract_address" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84' and e."from" = '\x0000000000000000000000000000000000000000') as iceth_amount,
    sum(e."value"/1e18) filter (where e."contract_address" = '\x1982b2F5814301d4e9a8b0201555376e62F82428' and e."to" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84') as aSTETH_amount,
    sum(e."value"/1e18) filter (where e."contract_address" = '\xF63B34710400CAd3e044cFfDcAb00a0f32E33eCf' and e."to" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84') as variableDebtWETH_amount
from        setprotocol_v2."DebtIssuanceModuleV2_evt_SetTokenIssued" i
left join   erc20."ERC20_evt_Transfer" e on i."evt_tx_hash" = e."evt_tx_hash"
where       "_setToken" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84'
group by    1,2
),

redeem_events as (
select
    i."evt_block_time",
    i."evt_tx_hash",
    sum(e."value"/1e18) filter (where e."contract_address" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84' and e."to" = '\x0000000000000000000000000000000000000000') as iceth_amount,
    sum(e."value"/1e18) filter (where e."contract_address" = '\x1982b2F5814301d4e9a8b0201555376e62F82428' and e."from" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84') as aSTETH_amount,
    sum(e."value"/1e18) filter (where e."contract_address" = '\xF63B34710400CAd3e044cFfDcAb00a0f32E33eCf' and e."from" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84') as variableDebtWETH_amount
from        setprotocol_v2."DebtIssuanceModuleV2_evt_SetTokenRedeemed" i
left join   erc20."ERC20_evt_Transfer" e on i."evt_tx_hash" = e."evt_tx_hash"
where       "_setToken" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84'
group by    1,2
),

all_events as (
select 
    evt_block_time, 
    evt_tx_hash, 	
    asteth_amount / iceth_amount as asteth,
    variabledebtweth_amount / iceth_amount as variabledebtweth
from issue_events
union
select 
    evt_block_time, 
    evt_tx_hash, 	
    asteth_amount / iceth_amount as asteth,
    variabledebtweth_amount / iceth_amount as variabledebtweth
from redeem_events
),

iceth_leverage_ratio as (
select
    hour,
    a.asteth,
    a.variabledebtweth,
    a.asteth / (a.asteth - a.variabledebtweth) as lev_ratio,
    a.asteth - a.variabledebtweth as parity_value
from    (
        select
            h.hour,
            (select evt_tx_hash from all_events where evt_block_time <= h.hour order by evt_block_time desc limit 1) as hash
        from        (select generate_series('2022-04-01 16:00', date_trunc('hour', now()), '1 hour') as hour) h
        ) t
inner join  all_events a on a.evt_tx_hash = t.hash
),

daily_fee_rates as (
select
    day,
    first_value(streaming_fee) over (partition by t.token_address, sf_part order by day asc) as streaming_fee,
    first_value(issue_fee) over (partition by t.token_address, if_part order by day asc) as issue_fee,
    first_value(redeem_fee) over (partition by t.token_address, rf_part order by day asc) as redeem_fee
from    (
        select
            d.day,
            t.token_address,
            x.streaming_fee,
            x.issue_fee,
            x.redeem_fee,
            sum(case when x.streaming_fee is null then 0 else 1 end) over (partition by t.token_address order by d.day asc) as sf_part,
            sum(case when x.issue_fee is null then 0 else 1 end) over (partition by t.token_address order by d.day asc) as if_part,
            sum(case when x.redeem_fee is null then 0 else 1 end) over (partition by t.token_address order by d.day asc) as rf_part
        from        (select token_address, date_trunc('day', min(block_time)) as min_day from dune_user_generated.indexcoop_fee_changes group by 1) t
        cross join  (select generate_series(date_trunc('day', (select min(block_time) from dune_user_generated.indexcoop_fee_changes)), date_trunc('day', now()), '1 day') as day) d
        left join   (
                    select 
                        token_address, 
                        date_trunc('day', block_time) as day, 
                        case when streaming_fee is null then 0 else streaming_fee end as streaming_fee , 
                        case when issue_fee is null then 0 else issue_fee end as issue_fee,
                        case when redeem_fee is null then 0 else redeem_fee end as redeem_fee,
                        row_number() over (partition by token_address, date_trunc('day', block_time) order by block_time desc, priority desc) as rnb
                    from    dune_user_generated.indexcoop_fee_changes
                    ) x on x.day = d.day and x.token_address = t.token_address and x.rnb = 1
        where       d.day >= t.min_day
        ) t
where   t.token_address = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84' -- icETH
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
from    (
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
                from        iceth_leverage_ratio i
                left join   rates r on i.hour = r.hour
                ) t0
        ) t1
left join   daily_fee_rates f on date_trunc('day', t1.hour) = f.day
)

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
    text((round("Net Yield vs ETH" * 100,2))) || '%' as net_yield_eth_txt,
    text((round(s.aave_apy * 100,2))) || '%' as aave_apy_txt,
    text((round(s.lido_apy * 100,2))) || '%' as lido_apy_txt,
    text((round("Streaming Fee" * 100,2))) || '%' as streaming_fee_txt,
    parity_value as "Parity Value ETH",
    parity_value * p.price as "Parity Value USD",
    parity_value * p1.price as "Net Asset Value"
from        summary s
left join   prices.layer1_usd p on p.minute = s.hour and p.symbol = 'ETH'
left join   prices.usd p1 on p1.minute = s.hour and p1.contract_address = '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'
where       s.hour >= '2022-04-08'

order by 1 desc
