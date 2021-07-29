/*
Jack's query: https://duneanalytics.com/queries/87944
don's query: https://duneanalytics.com/queries/92066
Wallet / Address
'\x5bc4249641b4bf4e37ef513f3fa5c63ecab34881' - MVI|ETH LP Staking Contract
'\x4d3c5db2c68f6859e0cd05d080979f597dd64bff' - MVI|ETH LP Token 
'\x0954906da0Bf32d5479e25f46056d22f08464cab' - INDEX Token
*/

-- for simplicity, copying the entire MVI_Liquidity Mining.sql CTE chain here to get the daily LP index rewards
-- A lot of the CTEs aren't strictly necessary but let's just keep it simple for now
with current_day as (
  select 
    ROW_NUMBER() OVER (ORDER BY day) AS rows
    , "day" as day
    , "amount_raw"/1e18 as amount
  from erc20."view_token_balances_daily"
  where "wallet_address" = '\x5bc4249641b4bf4e37ef513f3fa5c63ecab34881'
  and token_address = '\x4d3c5db2c68f6859e0cd05d080979f597dd64bff'
  limit 360
)
, prev_day as (
  select 
  1+ROW_NUMBER() OVER (ORDER BY day) AS rows
  , "day" as day
  , "amount_raw"/1e18 as amount
  from erc20."view_token_balances_daily"
  where "wallet_address" = '\x5bc4249641b4bf4e37ef513f3fa5c63ecab34881'
  and token_address = '\x4d3c5db2c68f6859e0cd05d080979f597dd64bff'
  limit 360
)
, stake_contract as (
  select --*, amount as "MVI|ETH LP", amount-amount2 as change
  c.day, c.amount as "MVI|ETH LP", c.amount-p.amount as change
  from current_day c left join prev_day p
  on c.rows = p.rows
  order by 1
)
, index_price as (
  SELECT 
  date_trunc('day', minute) AS day,
  avg(price) as price
  FROM prices.usd
  WHERE minute >= '2021-04-06 00:00' -- Date when the 1st MVI|ETH LP staked on the staking contract
  and contract_address ='\x0954906da0Bf32d5479e25f46056d22f08464cab' 
  group by 1
)
, weth_price as (
SELECT 
  date_trunc('day', minute) AS day,
  avg(price) as price
  FROM prices.usd
  WHERE minute >= '2021-04-06 00:00' --date when the 1st MVI|ETH LP staked on the staking contract
  and contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' 
  group by 1
)
, index_weth_price as (
  select ip.day, ip.price as "INDEX Price", wp.price as "WETH Price"
  from index_price ip
  left join weth_price wp
  on ip.day = wp.day
  order by 1 desc
)
, reserves_univ2 AS (
     SELECT day,
            latest_reserves[3]/1e18 AS "MVI amount",
            latest_reserves[4]/1e18 AS "ETH amount"
   FROM
     (SELECT date_trunc('day', evt_block_time) AS day,
     (SELECT MAX(ARRAY[evt_block_number, evt_index, reserve0, reserve1])) AS latest_reserves
         FROM uniswap_v2."Pair_evt_Sync"
         WHERE contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff'

      GROUP BY 1) AS day_reserves 
)
, lp_mint as (
    SELECT
    date_trunc('day',evt_block_time) as day,
    sum(value/1e18) as balance
    FROM erc20."ERC20_evt_Transfer"
    WHERE contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff' ------ minted token uniswap pool contract address
    AND "from" = '\x0000000000000000000000000000000000000000'
    group by 1
)
, lp_burn as (
    SELECT
    date_trunc('day',evt_block_time) as day,
    sum(-value/1e18) as balance
    FROM erc20."ERC20_evt_Transfer"
    WHERE contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff' ------ burned token uniswap pool contract address
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
    --group by 1
    order by 1
)
, mvi_eth_lp_price as (
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
, daily_contract_balances as (
    select lc.day, lc."MVI|ETH LP" as "LP Staking Contract", lc.change, iwp."INDEX Price", iwp."WETH Price", 
    ru."MVI amount" as "MVI in UNIv2",  
    ru."ETH amount" as "ETH in UNIv2",
    iwp."WETH Price"*(ru."ETH amount"*2) as univ2_tvl,
    lu.balance as "MVI|ETH at UNIv2",
    -- INDEX rewards for MVI farm is 110 
    --(((iwp."INDEX Price"*110)/(iwp."WETH Price"*(ru."ETH amount"*2)))*365)*100 as apr
    ((iwp."INDEX Price"*110)/(lc."MVI|ETH LP"* melp.mviethLP_price)) * 365 * 100 as apr,
    ((110)/(lc."MVI|ETH LP")) as daily_index_rewards_per_lp_token,
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
)

-- END copied block here

, index_reward_claimed AS ( -- INDEX rewards

    SELECT
        tr."to" AS address
        , min(evt_block_time) as first_claim_time
        , sum(-tr.value/1e18) AS amount
     FROM erc20."ERC20_evt_Transfer" tr 
     WHERE tr."from" = '\x5bc4249641b4bf4e37ef513f3fa5c63ecab34881' -- LP contract address
     and  contract_address = '\x0954906da0bf32d5479e25f46056d22f08464cab' -- INDEX token
     group by 1
)
, lp_staked AS (
    SELECT
        tr."from" AS address
        , evt_block_time
        , tr.value/1e18 AS amount
     FROM erc20."ERC20_evt_Transfer" tr
   WHERE tr."to" = '\x5bc4249641b4bf4e37ef513f3fa5c63ecab34881' --  MVI-ETH LP Stake Contract (entered)
   and contract_address = '\x4d3c5db2c68f6859e0cd05d080979f597dd64bff' -- LP token
)

, lp_withdrawn AS (
    SELECT
        tr."to" AS address
        , evt_block_time
        , -tr.value/1e18 as amount
     FROM erc20."ERC20_evt_Transfer" tr 
     WHERE tr."from" = '\x5bc4249641b4bf4e37ef513f3fa5c63ecab34881' -- MVI-ETH LP Stake Contract (left)
     and contract_address = '\x4d3c5db2c68f6859e0cd05d080979f597dd64bff'
)
, lp_transfers as (
    select *
    from lp_staked
    union
    select *
    from lp_withdrawn
)
, lp_transfers_daily as (
    select address
        , date_trunc('day', evt_block_time) as day
        , sum(amount) as net_change_lp_token
    from lp_transfers
    group by 1,2
)
, days as (
    select generate_series(min(day), date_trunc('day',now()), '1 day') as day
    from lp_transfers_daily
)
, lp_transfers_day AS (
    SELECT
        date_trunc('day', t.evt_block_time) as day
        , t.address
        , sum(t.amount) AS change 
    FROM lp_transfers t
    GROUP BY 1,2
)
, lp_balances_w_gap_days AS (
    SELECT
        day
        , address
        , sum(change) OVER (PARTITION BY address ORDER BY day) AS "balance"
        , lead(day, 1, now()) OVER (PARTITION BY address ORDER BY day) AS next_day
    FROM lp_transfers_day
)

, lp_balances_all_days AS (
    SELECT
        d.day
        , b.address
        , sum(b.balance) AS "balance"
    FROM lp_balances_w_gap_days b
    INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
    GROUP BY 1,2 --,3
    ORDER BY 1,2 --,3
)

, lp_balances_ranked as (
    select day
        , address
        , balance
        , rank() over (partition by address order by day desc)
    from lp_balances_all_days
)
, lp_current_balance as (
    select address
        , balance
    from lp_balances_ranked
    where rank = 1
)

, lp_daily_rewards as (
    select
        b.day
        , b.address
        , b.balance
        , b.balance * c.daily_index_rewards_per_lp_token as index_reward
    from lp_balances_all_days b
    inner join daily_contract_balances c on b.day = c.day
)

, approximate_index_earned as (
    select address
        , count(case when balance > 0 then 1 else null end) as num_days_earned
        , sum(index_reward) as total_index_rewards_earned
    from lp_daily_rewards
    group by 1
)
, after_first_claim_transfers as (

    SELECT
        tr."from" AS address
        , sum(tr.value / 1e18) AS amount
        , 'swap/transfer' AS type
    FROM erc20."ERC20_evt_Transfer" tr
    inner join index_reward_claimed irc on tr."from" = irc.address
    WHERE contract_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
    and evt_block_time >= irc.first_claim_time
    group by 1
)      
, index_claimed_vs_rewards as (
    select aie.address
        , aie.num_days_earned
        , aie.total_index_rewards_earned
        , ic.amount as rewards_claimed
    from approximate_index_earned aie
    left join index_reward_claimed as ic on aie.address = ic.address
)   
    
select bal.address
    , bal.balance as "LP Tokens Currently Staked"
    , coalesce(aie.num_days_earned, 0) as "Number of days earned rewards"
    , aie.total_index_rewards_earned as "Total INDEX rewards earned (estimate)"
    , coalesce(ic.amount, 0) as "Total INDEX rewards claimed (precise)"
    , coalesce(afct.amount, 0) as "INDEX tokens transferred after first claim"
from lp_current_balance bal
left join approximate_index_earned aie on bal.address = aie.address
left join index_reward_claimed as ic on bal.address = ic.address
left join after_first_claim_transfers afct on bal.address = afct.address