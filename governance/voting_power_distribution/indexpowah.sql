/*
https://dune.xyz/queries/110756

List of address: https://github.com/SetProtocol/index-deployments/blob/master/deployments/outputs/1-

        "0x10F87409E405c5e44e581A4C3F2eECF36AAf1f92", -  Proxy
        "0x0954906da0Bf32d5479e25f46056d22f08464cab", -- INDEX Token Address
        "0x3452a7f30a712e415a0674c0341d44ee9d9786f9", -- Uniswap INDEX/ETH LP
        "0xA73DF646512C82550C2b3C0324c4EEdEE53b400C", -- SushiSwap INDEX/ETH LP
        "0xc2EdaD668740f1aA35E4D8f227fB8E17dcA888Cd", -- SushiSwap: MasterChef LP Staking Pool 

        "0xB93b505Ed567982E2b6756177ddD23ab5745f309", -- Index Coop: DPI staking rewards v2
        "0x5bC4249641B4bf4E37EF513F3Fa5C63ECAB34881"  -- Index Coop: MVI staking rewards
  
        
        "0x1df4564A96aAc2B6633F1ce2f3092a11e57F6c19",   -- Vesting
        "0x0e800B09cBC50e2CCbb01C6A833c56Ef692F3e3E",   -- Vesting
        "0x2f3a28DF7f031695c52C680A4f9888D947d666B4",   -- Vesting
        "0x55c316DEA64D0B6CE20eAb843c821a10E3bdb91B",   -- Vesting
        "0xec5bc904ABb557781b16435E344a59D2218a6E17",   -- Vesting
        "0x7B15bB785167c610020B52bf4B790396D73bf8a0",   -- Vesting
        "0x7833Ba760D9FE0085E39c490f5A8c66565770cA5",   -- Vesting
        "0x3955ebF597154bD93d1Bf9b66BC571FeA3050c38",   -- Vesting
        "0x9CDBCBC17614C07EC857fA39995634107332E035",   -- Vesting
        "0xD0b396C37aC2AE6Eb207aE4a85ca0C3d549E09A0",   -- Vesting
        "0x43D75513e7182C9c9513850b3a716Ff36F90e132",   -- Vesting
        "0x0Def278718bB15eE2173C65fb24C131243fFcb83"    -- Vesting


Requirements:
- Create an interactive pie chart showing the INDEX token distribution.
- Capture VC, Set Labs, DFP, Full Time contributors (as a block), community non FT (as a block) and LP positions.

Deliverables for INDEX token:
- A series of pie charts.
- A top 20 holder table.

*/
with 

                            -- checking wallets that currently hold  INDEX except contracts/addresses stated below
wallets_with_index as (  
select * from erc20."view_token_balances_latest"
where token_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
and wallet_address != '\x8f06FBA4684B5E0988F215a47775Bb611Af0F986'-- 1st DPI LM Rewards
and wallet_address != '\xB93b505Ed567982E2b6756177ddD23ab5745f309'-- 2nd DPI LM Rewards
and wallet_address != '\x56d68212113AC6cF012B82BD2da0E972091940Eb'-- ETHFLI LM Rewards (not active yet)
and wallet_address != '\x5bC4249641B4bf4E37EF513F3Fa5C63ECAB34881'-- 2nd MVI LM Rewards
and wallet_address != '\xa73df646512c82550c2b3c0324c4eedee53b400c' -- INDEX on Sushiswap
and wallet_address != '\x3452a7f30a712e415a0674c0341d44ee9d9786f9' -- INDEX on Uniswap_v2
and wallet_address != '\x8c13148228765Ba9E84EAf940b0416a5e349A5e7' -- INDEX on Uniswap_v3 
and wallet_address != '\xd3d555bb655acba9452bfc6d7cea8cc7b3628c55' --- Set: rebalancer TBD if this needs to be included
)
                          
                            -- checking wallets that currently hold  UNI INDEX/ETH LP except contracts/addresses stated below
, wallets_with_uniindexlp as (  
select * from erc20."view_token_balances_latest"
where token_address = '\x3452a7f30a712e415a0674c0341d44ee9d9786f9'
and wallet_address != '\x8f06FBA4684B5E0988F215a47775Bb611Af0F986'-- 1st DPI LM Rewards
and wallet_address != '\xB93b505Ed567982E2b6756177ddD23ab5745f309'-- 2nd DPI LM Rewards
and wallet_address != '\x56d68212113AC6cF012B82BD2da0E972091940Eb'-- ETHFLI LM Rewards (not active yet)
and wallet_address != '\x5bC4249641B4bf4E37EF513F3Fa5C63ECAB34881'-- 2nd MVI LM Rewards
and wallet_address != '\xa73df646512c82550c2b3c0324c4eedee53b400c' -- INDEX on Sushiswap
and wallet_address != '\x3452a7f30a712e415a0674c0341d44ee9d9786f9' -- INDEX on Uniswap_v2
and wallet_address != '\x8c13148228765Ba9E84EAf940b0416a5e349A5e7' -- INDEX on Uniswap_v3 
and wallet_address != '\xd3d555bb655acba9452bfc6d7cea8cc7b3628c55' --- Set: rebalancer TBD if this needs to be included
)                                   

                            -- checking wallets that currently hold  Sushi  INDEX/ETH LP except contracts/addresses stated below
, wallets_with_susindexlp as (  
select * from erc20."view_token_balances_latest"
where token_address = '\xa73df646512c82550c2b3c0324c4eedee53b400c'
and wallet_address != '\x8f06FBA4684B5E0988F215a47775Bb611Af0F986'-- 1st DPI LM Rewards
and wallet_address != '\xB93b505Ed567982E2b6756177ddD23ab5745f309'-- 2nd DPI LM Rewards
and wallet_address != '\x56d68212113AC6cF012B82BD2da0E972091940Eb'-- ETHFLI LM Rewards (not active yet)
and wallet_address != '\x5bC4249641B4bf4E37EF513F3Fa5C63ECAB34881'-- 2nd MVI LM Rewards
and wallet_address != '\xa73df646512c82550c2b3c0324c4eedee53b400c' -- INDEX on Sushiswap
and wallet_address != '\x3452a7f30a712e415a0674c0341d44ee9d9786f9' -- INDEX on Uniswap_v2
and wallet_address != '\x8c13148228765Ba9E84EAf940b0416a5e349A5e7' -- INDEX on Uniswap_v3 
and wallet_address != '\xd3d555bb655acba9452bfc6d7cea8cc7b3628c55' --- Set: rebalancer TBD if this needs to be included
)                                    

, wallet_with_all as (
select wi.wallet_address, wi.amount_raw/1e18 as indextoken, wu.amount_raw/1e18 as unilptoken, ws.amount_raw/1e18 as sushilptoken
from wallets_with_index wi
left join wallets_with_uniindexlp wu
on wi.wallet_address = wu.wallet_address
left join wallets_with_susindexlp ws
on wi.wallet_address = ws.wallet_address
)

                                    -- checking Staking Reward Contracts that currently hold INDEX
, farms_with_index as (
select *
, 'farm_index' AS type
from erc20."view_token_balances_latest"
where token_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
and ( wallet_address = '\x5bC4249641B4bf4E37EF513F3Fa5C63ECAB34881'-- 2nd MVI LM Rewards
or  wallet_address = '\x8f06FBA4684B5E0988F215a47775Bb611Af0F986'-- 1st DPI LM Rewards
or  wallet_address = '\xB93b505Ed567982E2b6756177ddD23ab5745f309')-- 2nd DPI LM Rewards

)

                                            -- checking DEX that currently hold INDEX
, dex_with_index as (
select *
, 'dex_index' AS type
from erc20."view_token_balances_latest"
where token_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
and ( wallet_address = '\xa73df646512c82550c2b3c0324c4eedee53b400c' -- INDEX on Sushiswap
or  wallet_address = '\x3452a7f30a712e415a0674c0341d44ee9d9786f9' -- INDEX on Uniswap_v2
or wallet_address = '\x8c13148228765Ba9E84EAf940b0416a5e349A5e7') -- INDEX on Uniswap_v3
)

, masterchef_add as (                                    -- checking added INDEX|ETH LP from MasterChef 
select "user", sum(amount/1e18) as amount  from sushi."MasterChef_evt_Deposit"
where pid = 75 --- 75 is used by INDEX/ETH SLP
group by 1                                  
)  

, masterchef_remove as (                                -- checking removed  INDEX|ETH LP from MasterChef
select "user", -sum(amount/1e18) as amount from sushi."MasterChef_evt_Withdraw"
where pid = 75 --- 75 is used by INDEX/ETH SLP
group by 1                                      
)     
                                                        -- net_change of INDEX|ETH LP from MasterChef
, masterchef_net_temp as (
select * from masterchef_add
union all
select * from masterchef_remove
)
, masterchef_net as (
select "user", sum(amount) as amount, 'staked' as masterchef
from masterchef_net_temp
group by 1                                          
)
                                            --address that added INDEX to Uniswap LP
, uni_add_liquidity as (
select
--date_trunc('day', call_block_time) AS day,
"to" AS address
, sum("output_amountToken"/1e18) AS amount
, 'uniswap_lp' AS type
--call_tx_hash AS evt_tx_hash
from uniswap_v2."Router02_call_addLiquidityETH"
where token = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
group by 1

UNION ALL
        

select
"to" AS address
, sum(case    
    when "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then ("output_amountA"/1e18)
    when "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then ("output_amountB"/1e18)
    else 0
    end) as amount
    ,'uniswap_lp' AS type
    from uniswap_v2."Router02_call_addLiquidity"
where  "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
or "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
group by 1
        
UNION ALL

select
"to" AS address
,sum( case    
    when "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then ("output_amountA"/1e18)
    when "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then ("output_amountB"/1e18)
    else 0
    end) as amount
,    'uniswap_lp' as type
from uniswap_v2."Router01_call_addLiquidity"
where  "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
or "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
group by 1   
)

                                -- end of addresses the added INDEX to Uniswap LP
                                
                                -- addresses that removed INDEX from Uniswap LP
, uni_remove_liquidity as (                                

select
"to" as address
, -sum("output_amountToken"/1e18) as amount
, 'uniswap_lp' as type
from uniswap_v2."Router02_call_removeLiquidityETHWithPermit"
where token = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
group by 1        

UNION ALL
        
Select
 "to" as address
, -sum("output_amountToken"/1e18) as amount
, 'uniswap_lp' as type
from uniswap_v2."Router02_call_removeLiquidityETH"
where token = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
group by 1   

UNION ALL
        
select
"to" AS address
,sum(case
    when "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then -("output_amountA"/1e18)
    when "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then -("output_amountB"/1e18)
    else 0
    end) as amount
,'uniswap_lp' as type
from uniswap_v2."Router01_call_removeLiquidity"
where "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
or "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
group by 1

 UNION ALL
        
select
"to" as address
, sum(case
    when "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then -("output_amountA"/1e18)
    when "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then -("output_amountB"/1e18)
    else 0
    end) as amount
, 'uniswap_lp' as type
from uniswap_v2."Router02_call_removeLiquidity"
where "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
or "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
group by 1

UNION ALL
        
select
"to" as address
, sum(case
    when "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then -("output_amountA"/1e18)
    when "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then -("output_amountB"/1e18)
    else 0
    end) as amount
, 'uniswap_lp' as type
from uniswap_v2."Router02_call_removeLiquidityWithPermit"
where "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
or "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
group by 1
)
                                
                               --end of addresses that removed INDEX from Uniswap LP
                               
                                -- addresses that added INDEX to SUSHI LP
, sushi_add_liquidity as (
select
"to" as address
, sum("output_amountToken"/1e18) as amount
, 'sushi_lp' as type
from sushi."Router02_call_addLiquidityETH"
where token = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
group by 1

UNION ALL

select
"to" as address,
sum(case
when "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then ("output_amountA"/1e18)
when "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then ("output_amountB"/1e18)
else 0
end) as amount
, 'sushi_lp' as type
from sushi."Router02_call_addLiquidity"
where "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
or "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
group by 1
)                               
                                -- end of addresses that added INDEX to SUSHI LP 
                               
                                -- addresses that removed INDEX to SUSHI LP
, sushi_remove_liquidity as (
        
select
"to" as address
, -sum("output_amountToken"/1e18) AS amount
--date_trunc('day', call_block_time) AS evt_block_minute,
, 'sushi_lp' AS type
--call_tx_hash AS evt_tx_hash
from sushi."Router02_call_removeLiquidityETH"
where token = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
group by 1
        
UNION ALL
        
select
"to" as address
, -sum("output_amountToken"/1e18) AS amount
, 'sushi_lp' AS type
from sushi."Router02_call_removeLiquidityETHWithPermit"
where token = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
group by 1
        
UNION ALL
        
select
"to" AS address
, sum(case
    when "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then -("output_amountA"/1e18)
    when "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then -("output_amountB"/1e18)
    else 0
    end) as amount
, 'sushi_lp' AS type
from sushi."Router02_call_removeLiquidity"
where "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
or "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
group by 1

UNION ALL
        
select
"to" as address
, sum(case
    when "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then -("output_amountA"/1e18)
    when "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' then -("output_amountB"/1e18)
    else 0
    end) as amount
, 'sushi_lp' AS type
from sushi."Router02_call_removeLiquidityWithPermit"
where "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
or "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
group by 1
)
                                -- end of addresses that removed INDEX to SUSHI LP
                                
                                -- In/Out Summary for DEXes
, uni_summary_lp as (
select address, sum(amount) as amount, type 
from uni_add_liquidity
group by 1,3
union all
select address, sum(amount) as amount, type 
from uni_remove_liquidity
group by 1,3
)

, sushi_summary_lp as (
select address, sum(amount) as amount, type
from sushi_add_liquidity
group by 1,3
union all
select address, sum(amount) as amount, type
from sushi_remove_liquidity
group by 1,3
)
                                -- end of In/Out Summary for DEXes
                                -- DEXes net_change
, uni_net_lp as (                                
select address, sum(amount) as uniindex,
case 
    when sum(amount) > 0 then 'count as vote'
    else 'do not count' 
    end as indexpowah_uni
from uni_summary_lp
group by 1
)

, sushi_net_lp as (                                
select address, sum(amount) as sushiindex,
case 
    when sum(amount) > 0 then 'count as vote'
    else 'do not count' 
    end as indexpowah_sushi
from sushi_summary_lp
group by 1
)
                      -- count as vote if amount is > 0 else do not count
                     -- combine wallets with index and wallets that LPed

/*select ww.wallet_address, ww.amount, un.uniindex, un.indexpowah_uni, sn.sushiindex, sn.indexpowah_sushi, mn.amount as lpmasterchef, mn.masterchef
from wallets_with_index ww
left join uni_net_lp un
on ww.wallet_address = un.address
left join sushi_net_lp sn
on ww.wallet_address = sn.address
left join masterchef_net mn
on ww.wallet_address = mn."user"
*/
--select , sum(amount) as amount, 'staked' as masterchef

, all_summary as (
select wa.wallet_address, wa.indextoken, wa.unilptoken, wa.sushilptoken, un.uniindex, un.indexpowah_uni, sn.sushiindex, sn.indexpowah_sushi, mn.amount as lpmasterchef, mn.masterchef
from wallet_with_all wa
left join uni_net_lp un
on wa.wallet_address = un.address
left join sushi_net_lp sn
on wa.wallet_address = sn.address
left join masterchef_net mn
on wa.wallet_address = mn."user"
)


select *,
case
    when (indextoken >= 0 and unilptoken is null and  sushilptoken is null ) then indextoken
    when (indextoken >= 0 and unilptoken > 0 and uniindex >= 0 and  sushilptoken is null ) then indextoken + uniindex  -- unilptoken must be more than 0 before index can be counted as vote
    when (indextoken >= 0 and unilptoken > 0 and uniindex < 0 and  sushilptoken is null ) then indextoken
    when (indextoken >= 0 and unilptoken is null and  sushilptoken >= 0 and sushiindex >= 0) then indextoken + sushiindex  -- it is possible that sushilptoken (if 0) is staked in masterchef
    when (indextoken >= 0 and unilptoken is null and  sushilptoken >= 0 and sushiindex < 0) then indextoken
    when (indextoken >= 0 and unilptoken >= 0 and  sushilptoken >= 0 ) then indextoken + uniindex + sushiindex
    else indextoken
    end as total_power
from all_summary   
where indextoken >= 1 or unilptoken > 0 or sushilptoken > 0 or lpmasterchef > 0