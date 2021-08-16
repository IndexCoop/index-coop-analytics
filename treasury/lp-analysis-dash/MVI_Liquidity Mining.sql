/*
Don's copy: https://duneanalytics.com/queries/82789
Jack's copy: https://duneanalytics.com/queries/95299 

Wallet / Address
'\x5bc4249641b4bf4e37ef513f3fa5c63ecab34881' - MVI|ETH LP Staking Contract
'\x4d3c5db2c68f6859e0cd05d080979f597dd64bff' - MVI|ETH LP Token 
'\x0954906da0Bf32d5479e25f46056d22f08464cab' - INDEX Token

*/

with current_day as (
  select 
  ROW_NUMBER() OVER (ORDER BY day) AS rows,
  "day" as day,
  "amount_raw"/1e18 as amount
  from erc20."view_token_balances_daily"
  where "wallet_address" = '\x5bc4249641b4bf4e37ef513f3fa5c63ecab34881'
  and token_address = '\x4d3c5db2c68f6859e0cd05d080979f597dd64bff'
  limit 360
),
prev_day as (
  select 
  1+ROW_NUMBER() OVER (ORDER BY day) AS rows,
  "day" as day,
  "amount_raw"/1e18 as amount
  from erc20."view_token_balances_daily"
  where "wallet_address" = '\x5bc4249641b4bf4e37ef513f3fa5c63ecab34881'
  and token_address = '\x4d3c5db2c68f6859e0cd05d080979f597dd64bff'
  limit 360
),
stake_contract as (
  select --*, amount as "MVI|ETH LP", amount-amount2 as change
  c.day, c.amount as "MVI|ETH LP", c.amount-p.amount as change
  from current_day c left join prev_day p
  on c.rows = p.rows
  order by 1
),
index_price as (
  SELECT 
  date_trunc('day', minute) AS day,
  avg(price) as price
  FROM prices.usd
  WHERE minute >= '2021-04-06 00:00' -- Date when the 1st MVI|ETH LP staked on the staking contract
  and contract_address ='\x0954906da0Bf32d5479e25f46056d22f08464cab' 
  group by 1
),
weth_price as (
SELECT 
  date_trunc('day', minute) AS day,
  avg(price) as price
  FROM prices.usd
  WHERE minute >= '2021-04-06 00:00' --date when the 1st MVI|ETH LP staked on the staking contract
  and contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' 
  group by 1
),
index_weth_price as (
  select ip.day, ip.price as "INDEX Price", wp.price as "WETH Price"
  from index_price ip
  left join weth_price wp
  on ip.day = wp.day
  order by 1 desc
),
reserves_univ2 AS (
     SELECT day,
            latest_reserves[3]/1e18 AS "MVI amount",
            latest_reserves[4]/1e18 AS "ETH amount"
   FROM
     (SELECT date_trunc('day', evt_block_time) AS day,
     (SELECT MAX(ARRAY[evt_block_number, evt_index, reserve0, reserve1])) AS latest_reserves
         FROM uniswap_v2."Pair_evt_Sync"
         WHERE contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff'

      GROUP BY 1) AS day_reserves 
),
lp_mint as (
SELECT
date_trunc('day',evt_block_time) as day,
sum(value/1e18) as balance
FROM erc20."ERC20_evt_Transfer"
WHERE contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff' ------ minted token uniswap pool contract address
AND "from" = '\x0000000000000000000000000000000000000000'
group by 1
),
lp_burn as (
SELECT
date_trunc('day',evt_block_time) as day,
sum(-value/1e18) as balance
FROM erc20."ERC20_evt_Transfer"
WHERE contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff' ------ burned token uniswap pool contract address
AND "to" = '\x0000000000000000000000000000000000000000'
group by 1
),
lp_total as (
select * from lp_mint lm
union all
select * from lp_burn lb
),
lp_univ2 as (
SELECT 
distinct day,
sum(balance) over (order by day) as balance
from lp_total
--group by 1
order by 1
),
mvi_eth_lp_price as (
select lc.day,
iwp."WETH Price"*(ru."ETH amount"*2) as univ2_tvl,
(iwp."WETH Price"*(ru."ETH amount"*2))/lu.balance as mviethLP_price --1 LP Token in USD = (Total Value of the Liquidity Poll/Circulating Supply of Tokens)
from stake_contract lc
left join index_weth_price iwp
on lc.day = iwp.day
left join reserves_univ2 ru
on lc.day = ru.day
left join lp_univ2 lu
on lc.day = lu.day
)

select lc.day, lc."MVI|ETH LP" as "LP Staking Contract", lc.change, iwp."INDEX Price", iwp."WETH Price", 
ru."MVI amount" as "MVI in UNIv2",  
ru."ETH amount" as "ETH in UNIv2",
iwp."WETH Price"*(ru."ETH amount"*2) as univ2_tvl,
lu.balance as "MVI|ETH at UNIv2",
-- INDEX rewards for MVI farm is 110 
--(((iwp."INDEX Price"*110)/(iwp."WETH Price"*(ru."ETH amount"*2)))*365)*100 as apr
((iwp."INDEX Price"*110)/(lc."MVI|ETH LP"* melp.mviethLP_price)) * 365 * 100 as apr,
((110)/(lc."MVI|ETH LP")) as daily_index_rewards_per_lp,
melp.mviethLP_price as LPtoken_USD
from stake_contract lc
left join index_weth_price iwp
on lc.day = iwp.day
left join reserves_univ2 ru
on lc.day = ru.day
left join lp_univ2 lu
on lc.day = lu.day
left join mvi_eth_lp_price melp
on lc.day = melp.day
where lc.day >= '2021-04-07 00:00' -- date of MVI officially launch
