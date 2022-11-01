-- https://dune.com/queries/1426616/
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
)
, steth_price_join as (select lr.hour, 
lr.asteth, 
lr.variabledebtweth, 
lr.lev_ratio, 
d.median_price as stETH_price
from 
iceth_leverage_ratio lr
left join dex."view_token_prices" d on lr.hour = d.hour
where d.contract_address = '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'
), 
final_table as (
select s.hour, 
s.asteth, 
s.variabledebtweth, 
s.lev_ratio, 
s.stETH_price, 
e.median_price as ETH_price
from steth_price_join s 
left join dex."view_token_prices" e on s.hour = e.hour
where e.contract_address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
)
select *, 
asteth * steth_price as steth_deposit, 
variabledebtweth * eth_price as eth_debt, 
(asteth * steth_price) * 0.83 as deposit_value_at_liquidation,
(1 - ((variabledebtweth * eth_price)/((asteth * steth_price) * 0.83))) as Price_drop, 
steth_price * ((variabledebtweth * eth_price)/((asteth * steth_price) * 0.83)) as stETH_liquidation_price,
steth_price / eth_price as current_steth_eth_price,
steth_price * ((variabledebtweth * eth_price)/((asteth * steth_price) * 0.83)) / eth_price as liquidation_steth_eth_price
from final_table


