/*
https://dune.xyz/queries/110756

List of address: https://github.com/SetProtocol/index-deployments/blob/master/deployments/outputs/1-

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


                                            -- checking vesting contracts that can vote and currently hold INDEX 

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
, dex_net_lp as (                                
select address, sum(amount) as amount, type 
from uni_summary_lp
group by 1,3 

union all                            
--, sushi_net_lp as (                                
select address, sum(amount) as amount, type 
from sushi_summary_lp
group by 1,3
)
                                -- count as vote if amount is > 0 else do not count
, dex_summary_lp as (                               
select address as wallet_address, amount, type,
case 
    when amount > 0 then 'count as vote'
    else 'do not count' 
    end as indexpowah
from dex_net_lp
)
                                -- combined wallets_with_index and wallets that LPed
, dex_and_wallets as (
select wallet_address, amount
from dex_summary_lp
where indexpowah = 'count as vote'

union all

select wallet_address, amount
from wallets_with_index
)

select * from dex_and_wallets
where amount > 0