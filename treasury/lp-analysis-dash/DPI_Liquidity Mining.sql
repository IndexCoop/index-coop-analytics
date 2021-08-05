/*
Don: https://duneanalytics.com/queries/94971
Jack: https://dune.xyz/queries/97969

Wallet / Address
'\xB93b505Ed567982E2b6756177ddD23ab5745f309' - DPI|ETH LP Staking Contract
'\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' - DPI
'\0x4d5ef58aac27d99935e5b6b4a6778ff292059991' - DPI |ETH LP Token 
'\x0954906da0Bf32d5479e25f46056d22f08464cab' - INDEX Token

https://gov.indexcoop.com/t/iip-11-dpi-liquidity-mining-2/446  
December - 115920 INDEX

https://gov.indexcoop.com/t/iip-12-dpi-liquidity-mining-3/640
January - 75,000 INDEX 

https://gov.indexcoop.com/t/iip-14-dpi-liquidity-mining-4/770
February - 37,500 INDEX 

https://gov.indexcoop.com/t/iip-xxx-draft-dpi-liquidity-mining-5/949
March - 21,000

https://gov.indexcoop.com/t/iip-28-dpi-liquidity-mining-6/1145
APril - 21,700

https://gov.indexcoop.com/t/dpi-liquidity-mining-7/1441
May - 15,900

https://gov.indexcoop.com/t/dpi-liquidity-mining-june-2021/1647
June - 17,010

https://gov.indexcoop.com/t/iip-53-dpi-liquidity-mining-july-2021/1842
July - 12,758 
*/

with current_day as (
select 
 ROW_NUMBER() OVER (ORDER BY day) AS rows,
"day" as day,
"amount_raw"/1e18 as amount
from erc20."view_token_balances_daily"
where "wallet_address" = '\xB93b505Ed567982E2b6756177ddD23ab5745f309'
and token_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
limit 360
)

, prev_day as (
select 
1+ROW_NUMBER() OVER (ORDER BY day) AS rows,
 "day" as day,
"amount_raw"/1e18 as amount
from erc20."view_token_balances_daily"
where "wallet_address" = '\xB93b505Ed567982E2b6756177ddD23ab5745f309'
and token_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
limit 360
)


, stake_contract as (
select
c.day, c.amount as "DPI|ETH LP", c.amount-p.amount as change
from current_day c left join prev_day p
on c.rows = p.rows
order by 1
)

, reward_generate_days as (
    select generate_series('2020-12-06 00:00', date_trunc('day',now()), '1 day') as day
  --  from lp_transfers_daily
    )


, index_daily_rewards as (
select day,
case 
when day >= '2020-12-06 00:00' and day <= '2021-01-06 00:00' then 3864 -- https://gov.indexcoop.com/t/iip-11-dpi-liquidity-mining-2/446 |  December - 115920 INDEX
when day >= '2021-01-07 00:00' and day <= '2021-02-05 00:00' then 2500 -- https://gov.indexcoop.com/t/iip-12-dpi-liquidity-mining-3/640  | January - 75,000 INDEX 
when day >= '2021-02-06 00:00' and day <= '2021-03-08 00:00' then 1250 -- https://gov.indexcoop.com/t/iip-14-dpi-liquidity-mining-4/770  | February - 37,500 INDEX 
when day >= '2021-03-09 00:00' and day <= '2021-04-11 00:00' then 700 -- https://gov.indexcoop.com/t/iip-xxx-draft-dpi-liquidity-mining-5/949  | March - 21,000 INDEX
when day >= '2021-04-12 00:00' and day <= '2021-05-11 00:00' then 700  -- https://gov.indexcoop.com/t/iip-28-dpi-liquidity-mining-6/1145  | April - 21,000
when day >= '2021-05-12 00:00' and day <= '2021-06-07 00:00' then 530 -- https://gov.indexcoop.com/t/dpi-liquidity-mining-7/1441    |    May - 15,900
when day >= '2021-06-08 00:00' and day <= '2021-07-08 00:00' then 567 -- https://gov.indexcoop.com/t/dpi-liquidity-mining-june-2021/1647 | June - 17,010
when day >= '2021-07-08 00:00' and day <= '2021-08-10 00:00' then 425   -- https://gov.indexcoop.com/t/iip-53-dpi-liquidity-mining-july-2021/1842 | July - 12,758 
end as index_daily_reward
from reward_generate_days
)

, index_temp as (
select *,
case
when token_a_amount > 0 then  "usd_amount"/"token_a_amount" 
end as usdprice 
from dex.trades
where token_a_symbol = 'INDEX'
--limit 1000
)

, index_price as (
SELECT 
date_trunc('day',block_time) as day,
avg(usdprice) as price
--date_trunc('day', minute) AS day,
--avg(price) as price
FROM index_temp
--WHERE minute >= '2020-12-06 00:00' -- Date when the 1st DPI|ETH LP staked on the staking contract
--and contract_address ='\x0954906da0Bf32d5479e25f46056d22f08464cab' 
group by 1
)


, weth_price as (
SELECT 
date_trunc('day', minute) AS day,
avg(price) as price
FROM prices.usd
WHERE minute >= '2020-12-06 00:00' --date when the 1st DPI|ETH LP staked on the staking contract
and contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' 
group by 1
)

, index_weth_price as (--INDEX and WETH Price in USD
select ip.day, ip.price as "INDEX Price", wp.price as "WETH Price"
from index_price ip
left join weth_price wp
on ip.day = wp.day
order by 1 desc
)

, reserves_univ2 AS ( -- daily DPI and ETH in UniV2
     SELECT day,
            latest_reserves[3]/1e18 AS "DPI amount",
            latest_reserves[4]/1e18 AS "ETH amount"
   FROM
     (SELECT date_trunc('day', evt_block_time) AS day,
     (SELECT MAX(ARRAY[evt_block_number, evt_index, reserve0, reserve1])) AS latest_reserves
         FROM uniswap_v2."Pair_evt_Sync"
         WHERE contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'

      GROUP BY 1) AS day_reserves 
)

, lp_mint as (
SELECT
date_trunc('day',evt_block_time) as day,
sum(value/1e18) as balance
FROM erc20."ERC20_evt_Transfer"
WHERE contract_address = '\x4d5ef58aAc27d99935E5b6B4A6778ff292059991' ------ minted token uniswap pool contract address
AND "from" = '\x0000000000000000000000000000000000000000'
group by 1
)

, lp_burn as (
SELECT
date_trunc('day',evt_block_time) as day,
sum(-value/1e18) as balance
FROM erc20."ERC20_evt_Transfer"
WHERE contract_address = '\x4d5ef58aAc27d99935E5b6B4A6778ff292059991' ------ burned token uniswap pool contract address
AND "to" = '\x0000000000000000000000000000000000000000'
group by 1
)

, lp_total as (
select * from lp_mint lm
union all
select * from lp_burn lb
)

, lp_univ2 as (
SELECT 
distinct day,
sum(balance) over (order by day) as balance
from lp_total
order by 1
)

, dpi_eth_lp_price as ( --
select lc.day,
iwp."WETH Price"*(ru."ETH amount"*2) as univ2_tvl,
(iwp."WETH Price"*(ru."ETH amount"*2))/lu.balance as dpiethlp_price --1 LP Token in USD = (Total Value of the Liquidity Pool/Circulating Supply of Tokens)
from stake_contract lc
left join index_weth_price iwp
on lc.day = iwp.day
left join reserves_univ2 ru
on lc.day = ru.day
left join lp_univ2 lu
on lc.day = lu.day
)

, final as (
select
lc.day, lc."DPI|ETH LP" as "LP at Contract", lc.change, iwp."INDEX Price", iwp."WETH Price", 
ru."DPI amount" as "DPI in UNIv2",  
ru."ETH amount" as "ETH in UNIv2",
iwp."WETH Price"*(ru."ETH amount"*2) as univ2_tvl,
lu.balance as "DPI|ETH at UNIv2",
--((iwp."INDEX Price"*425)/(lc."DPI|ETH LP"* delp.dpiethlp_price)) * 365 * 100 as apr,
delp.dpiethlp_price as LPtoken_USD
from stake_contract lc
left join index_weth_price iwp
on lc.day = iwp.day
left join reserves_univ2 ru
on lc.day = ru.day
left join lp_univ2 lu
on lc.day = lu.day
left join dpi_eth_lp_price delp
on lc.day = delp.day
where lc.day >= '2020-12-06 00:00'  --date when the 1st DPI|ETH LP staked on the staking contract
)

select 
f.day,
f."LP at Contract",
f.change,
f."INDEX Price",
f."WETH Price",
f."DPI in UNIv2",
f."ETH in UNIv2",
f.univ2_tvl,
f."DPI|ETH at UNIv2",
f.LPtoken_USD,
idr.index_daily_reward,
((f."INDEX Price"*idr.index_daily_reward)/(f."LP at Contract"* f.LPtoken_USD)) * 365 * 100 as apr,
 ((idr.index_daily_reward)/(f."LP at Contract")) as daily_index_rewards_per_lp_token
from final f
left join index_daily_rewards idr
on f.day = idr.day
