-- https://dune.xyz/queries/313689
-- TODO: split out the noncirculating addresses into its own day-level balance query to minimize the number of row comparisons
CREATE OR REPLACE view dune_user_generated.indexcoop_index_supply AS

with

param as ( select
'\x0954906da0Bf32d5479e25f46056d22f08464cab'::bytea as token_address
),

days as ( select
    generate_series('2020-10-06', current_date, '1 day') as day
),

--------------------------------------------------------------
-- Adddresses Not Included in Circulating Supply
--------------------------------------------------------------
addresses as ( select * from (values
--  Address                                         Name
    ('\x9467cfadc9de245010df95ec6a585a506a8ad5fc'::bytea, 'Index Coop: MultiSig'),
    ('\xd89c642e52bd9c72bcc0778bcf4de307cc48e75a'::bytea, 'Index Coop: Community Treasury Year 2 Vesting'),
    ('\xf64d061106054fe63b0aca68916266182e77e9bc'::bytea, 'Index Coop: Set Labs Year 1 Vesting'),
    ('\x71f2b246f270c6af49e2e514ca9f362b491fbbe1'::bytea, 'Index Coop: Community Treasury Year 3 Vesting'),
    ('\x4c11dfd35a4fe079b41d5d9729ed34c00d487712'::bytea, 'Index Coop: Set Labs Year 2 Vesting'),
    ('\x0d627ca04a97219f182dab0dc2a23fb4a5b02a9d'::bytea, 'Index Coop: Set Labs Year 3 Vesting'),
    ('\x66a7d781828b03ee1ae678cd3fe2d595ba3b6000'::bytea, 'Index Coop: Methodologist Vesting'),
    ('\x5c29aa6761803bcfda7f683eaa0ff9bddda3649d'::bytea, 'DeFi Pulse: Vesting Year 1'),
    ('\xce3c6312385fcf233ab0de574b0cb1a588566c3f'::bytea, 'DeFi Pulse: Vesting Year 2'),
    ('\x0f58793e8cf39d6b60919ffaf773a7f95a568146'::bytea, 'Index Coop: DeFi Pulse Year 3 Vesting'),
    ('\xdd111f0fc07f4d89ed6ff96dbab19a61450b8435'::bytea, 'Index Coop: Early Community Rewards'),
    ('\x26e316f5b3819264df013ccf47989fb8c891b088'::bytea, 'Index Coop: Community Treasury Year 1 Vesting'),
    ('\xe2250424378b6a6dc912f5714cfd308a8d593986'::bytea, 'Index Treasury Committee'),
    ('\xb93b505ed567982e2b6756177ddd23ab5745f309'::bytea, 'Index Coop: DPI Staking Rewards'),
    ('\x8f06fba4684b5e0988f215a47775bb611af0f986'::bytea, 'Initial Liquidity Mining Rewards')
    ) AS t (address, description)
),

--------------------------------------------------------------
-- Determine the Total Daily Supply
-------------------------------------------------------------- 

total_supply_change as (
SELECT 
    date_trunc('day',"evt_block_time") AS day,
    sum(value/1e18) AS amount
FROM        erc20."ERC20_evt_Transfer"
WHERE       contract_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
AND         "from" = '\x0000000000000000000000000000000000000000'
GROUP BY    day

union

SELECT 
    date_trunc('day',"evt_block_time") AS day,
    -SUM(value/1e18) AS amount
FROM erc20."ERC20_evt_Transfer"
WHERE contract_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
AND "to" = '\x000000000000000000000000000000000000dead'
GROUP BY 1
),

total_supply as (
select
    d.day,
    sum(tsc.amount) over (order by d.day asc rows between unbounded preceding and current row) as units
from        days d
left join   total_supply_change tsc on d.day = tsc.day
),

--------------------------------------------------------------
-- Determine the Circulating Daily Supply
-------------------------------------------------------------- 

summary as (
select
    d.day,
    total.units as total_supply,
    total.units - COALESCE(SUM(nc.amount),0) as circulating_supply
from        days d
left join   erc20.view_token_balances_daily nc on d.day = nc.day
inner join  param on nc.token_address = param.token_address
inner join  addresses nc.wallet_address = addresses.address
left join   total_supply total on d.day = total.day
group by    d.day, total_supply
order by    day desc
)

select * from summary
