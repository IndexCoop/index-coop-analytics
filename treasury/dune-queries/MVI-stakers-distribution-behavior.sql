/*
https://duneanalytics.com/queries/86838

Wallet / Address
'\x5bc4249641b4bf4e37ef513f3fa5c63ecab34881' - MVI|ETH LP Staking Contract
'\x4d3c5db2c68f6859e0cd05d080979f597dd64bff' - MVI|ETH LP Token 
'\x0954906da0Bf32d5479e25f46056d22f08464cab' - INDEX Token

*/
WITH index_reward AS ( -- INDEX rewards
SELECT
    tr."to" AS address,
    sum(-tr.value/1e18) AS amount
FROM erc20."ERC20_evt_Transfer" tr 
WHERE tr."from" = '\x5bc4249641b4bf4e37ef513f3fa5c63ecab34881' 
and  contract_address = '\x0954906da0bf32d5479e25f46056d22f08464cab'
group by 1
),

lp_staked AS ( --  MVI-ETH LP Stake Contract (entered)
SELECT
    tr."from" AS address,
    sum(tr.value/1e18) AS amount
FROM erc20."ERC20_evt_Transfer" tr
WHERE tr."to" = '\x5bc4249641b4bf4e37ef513f3fa5c63ecab34881' 
and contract_address = '\x4d3c5db2c68f6859e0cd05d080979f597dd64bff'
group by 1
),

lp_withdrawn AS ( -- MVI-ETH LP Stake Contract (left)
SELECT
    tr."to" AS address,
    sum(-tr.value/1e18) as amount
FROM erc20."ERC20_evt_Transfer" tr 
WHERE tr."from" = '\x5bc4249641b4bf4e37ef513f3fa5c63ecab34881' 
and contract_address = '\x4d3c5db2c68f6859e0cd05d080979f597dd64bff'
group by 1
),

all_transfers as (
SELECT  
    ls.address,  
    ls.amount as "Staked LP", 
    lw.amount as "Withdrawn LP", 
    ir.amount as "rewards", 
    (ls.amount+lw.amount) as remaining 
FROM lp_staked ls
left join lp_withdrawn lw
on ls.address = lw.address
left join index_reward ir
on ls.address = ir.address
GROUP BY 1,2,3,4,5
order by 5 desc
),

farmers as(
select address, 
    "Staked LP", 
    coalesce("Withdrawn LP",0) as "Withdrawn LP",
    coalesce("rewards",0) as rewards,
    "Staked LP" + coalesce("Withdrawn LP",0)  as "Remaining LP"
from all_transfers
),


temp_table as (
select f.address, 
    f."Remaining LP", 
    f.rewards 
from farmers f

),

transfers as (
SELECT
    tr."to" AS address,
    sum(tr.value / 1e18) AS amount,
    'swap/transfer' AS type
FROM erc20."ERC20_evt_Transfer" tr
WHERE contract_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
and evt_block_time > '2021-04-06 00:00' --start date of MVI-ETH LP Staking contract
group by 1
    )      
    
    
select 
    tt.address,                     -- stakers address
    tt."Remaining LP",              -- remaining lp token on staking contract
    tt.rewards,                     -- rewards claimed (INDEX Token)
coalesce(t.amount,0) as amount,     -- amount swapped or transffered to another wallet
case
    when t.amount is null then 'hodl'
    else 'spent/swapped'
end as type
from temp_table tt
left join transfers t
on tt.address = t.address
    