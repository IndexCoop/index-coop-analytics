/*
Don: https://duneanalytics.com/queries/94971
Jack: https://dune.xyz/queries/97974

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

-- Copied the query from https://duneanalytics.com/queries/94971

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

, daily_contract_balances as (
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
((f."INDEX Price"*idr.index_daily_reward)/(f."LP at Contract"* f.LPtoken_USD)) * 365 * 100 as apr,
 ((idr.index_daily_reward)/(f."LP at Contract")) as daily_index_rewards_per_lp_token
from final f
left join index_daily_rewards idr
on f.day = idr.day
)

-- END copied block here



, index_reward_claimed AS ( -- INDEX rewards

    SELECT
    tr."to" AS address
    , min(evt_block_time) as first_claim_time
    , sum(-tr.value/1e18) AS amount
     FROM erc20."ERC20_evt_Transfer" tr 
     WHERE tr."from" = '\xB93b505Ed567982E2b6756177ddD23ab5745f309' -- LP contract address
     and  contract_address = '\x0954906da0bf32d5479e25f46056d22f08464cab' -- INDEX token
     group by 1
)
, lp_staked AS (
    SELECT
    tr."from" AS address
    , evt_block_time
    , tr.value/1e18 AS amount
     FROM erc20."ERC20_evt_Transfer" tr
   WHERE tr."to" = '\xB93b505Ed567982E2b6756177ddD23ab5745f309' -- DPI|ETH LP Staking Contract (staked)
   and contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991' -- DPI |ETH LP Token 
)

, lp_withdrawn AS (
    SELECT
    tr."to" AS address,
    evt_block_time,
    -tr.value/1e18 as amount
     FROM erc20."ERC20_evt_Transfer" tr 
     WHERE tr."from" = '\xB93b505Ed567982E2b6756177ddD23ab5745f309' -- DPI|ETH LP Staking Contract (withdrawn)
     and contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991' -- DPI |ETH LP Token 
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
        date_trunc('day', t.evt_block_time) as day,
        t.address,
        sum(t.amount) AS change 
    FROM lp_transfers t
    GROUP BY 1,2
)
, lp_balances_w_gap_days AS (
    SELECT
        day,
        address,
        sum(change) OVER (PARTITION BY address ORDER BY day) AS "balance",
        lead(day, 1, now()) OVER (PARTITION BY address ORDER BY day) AS next_day
    FROM lp_transfers_day
)

, lp_balances_all_days AS (
    SELECT
        d.day,
        b.address,
        sum(b.balance) AS "balance"
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
    
, lp_summary as (select bal.address
    , bal.balance as "LP Tokens Currently Staked"
    , coalesce(aie.num_days_earned, 0) as "Number of days earned rewards"
    , aie.total_index_rewards_earned as "Total INDEX rewards earned (estimate)"
    , coalesce(ic.amount, 0) as "Total INDEX rewards claimed (precise)"
    , coalesce(afct.amount, 0) as "INDEX tokens transferred after first claim"
    , case when coalesce(afct.amount, 0) = 0 then 'Hodler' else 'Seller' end as account_type
    , row_number() over (order by bal.balance desc) as lp_rank
from lp_current_balance bal
left join approximate_index_earned aie on bal.address = aie.address
left join index_reward_claimed as ic on bal.address = ic.address
left join after_first_claim_transfers afct on bal.address = afct.address
)

select sum(case when lp_rank <= 10 then "LP Tokens Currently Staked" else 0 end) as "LP Token Balance for Top 10"
    , sum("LP Tokens Currently Staked") as "Total LP Tokens Staked"
    , sum(case when lp_rank <= 10 then "LP Tokens Currently Staked" else 0 end) * 100 
        / sum("LP Tokens Currently Staked") as "LP Token Concentration in Top 10"
    , sum("Total INDEX rewards earned (estimate)" + "Total INDEX rewards claimed (precise)") as "Outstanding INDEX to be claimed"
    , sum(case when account_type = 'Hodler' then "Total INDEX rewards earned (estimate)" + "Total INDEX rewards claimed (precise)"
        else 0 end) as "Outstanding INDEX to be claimed - Hodlers"
    , sum(case when account_type = 'Seller' then "Total INDEX rewards earned (estimate)" + "Total INDEX rewards claimed (precise)"
        else 0 end) as "Outstanding INDEX to be claimed - Sellers"
from lp_summary