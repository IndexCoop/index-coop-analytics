/*
https://dune.xyz/queries/117913

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

                    -- total number of INDEX|ETH UniLP Token over time
uni_lp_mint as (
select
date_trunc('day',evt_block_time) as day,
sum(value/1e18) as balance
from erc20."ERC20_evt_Transfer"
where contract_address = '\x3452a7f30a712e415a0674c0341d44ee9d9786f9' ------ minted token uniswap pool contract address
and "from" = '\x0000000000000000000000000000000000000000'
group by 1
)

, uni_lp_burn as (
select
date_trunc('day',evt_block_time) as day,
sum(-value/1e18) as balance
from erc20."ERC20_evt_Transfer"
where contract_address = '\x3452a7f30a712e415a0674c0341d44ee9d9786f9' ------ burned token uniswap pool contract address
and "to" = '\x0000000000000000000000000000000000000000'
group by 1
)

, uni_lp_total as (
select * from uni_lp_mint lm
union all
select * from uni_lp_burn lb
)

, generate_gap_uni_day as (
select day, sum(balance)as index_unilp from uni_lp_total
group by 1 

)
, generate_uni_days as (
select generate_series(min(day), date_trunc('day',now()), '1 day') as day
from uni_lp_total
)

, total_unilp_alldays as (
select gu.day, coalesce(gg.index_unilp, 0) as net_lp, sum(gg.index_unilp) over (order by gu.day) as total_unilp
from generate_uni_days gu
left join generate_gap_uni_day gg
on gu.day = gg.day
)
                    -- end of total number of INDEX|ETH UniLP Token over time

                    -- Total INDEX, ETH, INDEX|ETH UniLP and index_per_lp over time
, index_reserves_univ2 AS ( 
     SELECT day,
            latest_reserves[3]/1e18 AS "INDEX amount",
            latest_reserves[4]/1e18 AS "ETH amount"
   FROM
     (SELECT date_trunc('day', evt_block_time) AS day,
     (SELECT MAX(ARRAY[evt_block_number, evt_index, reserve0, reserve1])) AS latest_reserves
         FROM uniswap_v2."Pair_evt_Sync"
         WHERE contract_address = '\x3452a7f30a712e415a0674c0341d44ee9d9786f9'

      GROUP BY 1) AS day_reserves 
)

, uni_lp_summary_alldays as (
select  tu.day, ir."INDEX amount", ir."ETH amount", tu.total_unilp, ir."INDEX amount"/tu.total_unilp as index_per_lp
from total_unilp_alldays tu
left join index_reserves_univ2 ir
on tu.day = ir.day
)
                    -- end of Total INDEX, ETH, INDEX|ETH UniLP and index_per_lp over time
                    -- current voting power from UNILP holders                 
, voting_from_unilp as (
select ul.day, e.wallet_address, e.amount_raw/1e18 as unilp, (e.amount_raw/1e18)*(ul.index_per_lp) as index_votingpow
from uni_lp_summary_alldays ul
left join erc20."view_token_balances_daily" e
on ul.day = e.day
where e.token_address = '\x3452A7f30A712e415a0674C0341d44eE9D9786F9'
and e.day >= now()- interval '1 day'
and e.amount_raw/1e18 > 0
)
                    -- end of current voting power from UniLp holders    


                    -- total number of UNI INDEX|ETH SLP Token over time
, s_lp_mint as (
select
date_trunc('day',evt_block_time) as day,
sum(value/1e18) as balance
from erc20."ERC20_evt_Transfer"
where contract_address = '\xA73DF646512C82550C2b3C0324c4EEdEE53b400C' ------ minted token uniswap pool contract address
and "from" = '\x0000000000000000000000000000000000000000'
group by 1
)

, s_lp_burn as (
select
date_trunc('day',evt_block_time) as day,
sum(-value/1e18) as balance
from erc20."ERC20_evt_Transfer"
where contract_address = '\xA73DF646512C82550C2b3C0324c4EEdEE53b400C' ------ burned token uniswap pool contract address
and "to" = '\x0000000000000000000000000000000000000000'
group by 1
)

, s_lp_total as (
select * from s_lp_mint lm
union all
select * from s_lp_burn lb
)

, generate_gap_s_day as (
select day, sum(balance)as index_slp from s_lp_total
group by 1 

)
, generate_s_days as (
select generate_series(min(day), date_trunc('day',now()), '1 day') as day
from s_lp_total
)

, total_slp_alldays as (
select gu.day, coalesce(gg.index_slp, 0) as net_lp, sum(gg.index_slp) over (order by gu.day) as total_slp
from generate_s_days gu
left join generate_gap_s_day gg
on gu.day = gg.day
)
                    -- end of total number of INDEX|ETH SLP Token over time

                    -- Total INDEX, ETH, INDEX|ETH SLP and index_per_lp over time
, index_reserves_sushi AS ( 
     SELECT day,
            latest_reserves[3]/1e18 AS "INDEX amount",
            latest_reserves[4]/1e18 AS "ETH amount"
   FROM
     (SELECT date_trunc('day', evt_block_time) AS day,
     (SELECT MAX(ARRAY[evt_block_number, evt_index, reserve0, reserve1])) AS latest_reserves
         FROM sushi."Pair_evt_Sync"
         WHERE contract_address = '\xA73DF646512C82550C2b3C0324c4EEdEE53b400C'

      GROUP BY 1) AS day_reserves 
)

, s_lp_summary_alldays as (
select  tu.day, ir."INDEX amount", ir."ETH amount", tu.total_slp, ir."INDEX amount"/tu.total_slp as index_per_lp
from total_slp_alldays tu
left join index_reserves_sushi ir
on tu.day = ir.day
)
                    -- end of Total INDEX, ETH, INDEX|ETH SLP and index_per_lp over time
                    -- current voting power from SLP holders   
, temp_voting_from_slp as (
select ul.day, e.wallet_address, e.amount_raw/1e18 as slp, ul.index_per_lp, (e.amount_raw/1e18)*(ul.index_per_lp) as index_votingpow
from s_lp_summary_alldays ul
left join erc20."view_token_balances_daily" e
on ul.day = e.day
where e.token_address = '\xA73DF646512C82550C2b3C0324c4EEdEE53b400C'
and e.day >= now()- interval '1 day'
)
                                        
, walllets_with_slp_exposure as (
select * from erc20."view_token_balances_latest"
where "token_address" = '\xA73DF646512C82550C2b3C0324c4EEdEE53b400C'
and wallet_address != '\xc2edad668740f1aa35e4d8f227fb8e17dca888cd'-- (SushiSwap: MasterChef LP Staking Pool) contract address (cannot vote)
and wallet_address != '\x95c69c3220b31b843f1cf20bee5c53fcde7fc12e'-- contract address (cannot vote)
and wallet_address != '\x280ac711bb99de7c73fb70fb6de29846d5e4207f'-- contract address (cannot vote)
and wallet_address != '\xe11fc0b43ab98eb91e9836129d1ee7c3bc95df50'--  SushiSwap: SushiMaker contract address (cannot vote)
and wallet_address != '\x88ad09518695c6c3712ac10a214be5109a655671'-- POA Network: xDAI OmniBridge contract address (cannot vote)
)
                                -- checking added INDEX|ETH LP from MasterChef 
, masterchef_add as (                                    
select "user", sum(amount/1e18) as amount  from sushi."MasterChef_evt_Deposit"
where pid = 75 --- 75 is used by INDEX/ETH SLP
group by 1                                  
)  

                               -- checking removed  INDEX|ETH LP from MasterChef
, masterchef_remove as (                         
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
select "user", sum(amount) as amount 
from masterchef_net_temp
group by 1      
)
                                        -- checking wallet with SLP and Masterchef stakers
, wallet_and_masterchef_slp as (
select ws.wallet_address, ws.amount_raw/1e18 as slp_wallet, coalesce(mn.amount, 0) as slp_staked
--, sum(mn.amount) over (order by mn."user")
from  walllets_with_slp_exposure ws
left join masterchef_net mn
on ws.wallet_address = mn."user"
)

, voting_from_slp as (
select wm.wallet_address, wm.slp_wallet, wm.slp_staked, vs.index_per_lp, 
((wm.slp_wallet + wm.slp_staked)*vs.index_per_lp)  as index_votingpow
from wallet_and_masterchef_slp wm
left join temp_voting_from_slp vs
on wm.wallet_address = vs.wallet_address
)

                            -- checking wallets that currently hold  INDEX except contracts/addresses stated below
, wallets_with_index as (  
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
and wallet_address != '\x0000000000000000000000000000000000000000' --- mint/burn address
)                     

, temp as (
select ww.wallet_address, ww."amount_raw"/1e18 as indextoken, coalesce(vu.index_votingpow, 0) as univotingpower, coalesce(vs.index_votingpow, 0) as sushivotingpower
--(ww."amount_raw"/1e18 + vu.index_votingpow + vs.index_votingpow) total_votingpower
from wallets_with_index ww
left join voting_from_slp vs
on ww.wallet_address = vs.wallet_address
left join voting_from_unilp vu
on ww.wallet_address = vu.wallet_address
)

select wallet_address, indextoken, sushivotingpower, univotingpower, (indextoken + sushivotingpower + univotingpower) as total_votingpower
from temp





