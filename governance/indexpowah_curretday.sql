/*
https://dune.xyz/queries/126630

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

                    --checking current INDEX token Balance for all wallet
with 
index_transfers as (
    --ERC20 Tokens
    select
        date_trunc('day', evt_block_time) as day,
        "to" as address,
        sum(value/1e18) as txn
 --       sum(value/1e18) over (partition by "to" order by evt_block_time asc)AS amount
    from erc20."ERC20_evt_Transfer"
    where "contract_address" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
    and evt_block_time >= '2020-10-06 00:00' -- contract creation date
    group by 1,2
    
    union all

    select
        date_trunc('day', evt_block_time) as day,
        "from" as address,
        sum(-value/1e18) as txn
  --      sum(-value/1e18) over (partition by "from" order by evt_block_time asc)AS amount
    from erc20."ERC20_evt_Transfer"
    where "contract_address" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
    and evt_block_time >= '2020-10-06 00:00'  -- contract creation date
    group by 1,2
)

, index_token_balance as (
select  *
    , sum(txn) over (partition by address order by day asc) as index_bal
    , rank() over (partition by address order by day desc)
from index_transfers
)
               -- end of checking current INDEX token Balance for all wallet
              --checking current UNI INDEX|ETH LP token Balance for all wallet

, unilp_transfers as (
    --ERC20 Tokens
    select
        date_trunc('day', evt_block_time) as day,
        "to" as address,
        sum(value/1e18) as txn
 --       sum(value/1e18) over (partition by "to" order by evt_block_time asc)AS amount
    from erc20."ERC20_evt_Transfer"
    where "contract_address" = '\x3452a7f30a712e415a0674c0341d44ee9d9786f9'
    and evt_block_time >= '2020-10-06 00:00' -- contract creation date
    group by 1,2
    
    union all

    select
        date_trunc('day', evt_block_time) as day,
        "from" as address,
        sum(-value/1e18) as txn
  --      sum(-value/1e18) over (partition by "from" order by evt_block_time asc)AS amount
    from erc20."ERC20_evt_Transfer"
    where "contract_address" = '\x3452a7f30a712e415a0674c0341d44ee9d9786f9'
    and evt_block_time >= '2020-10-06 00:00'  -- contract creation date
    group by 1,2
)

, unilp_token_balance as (
select  *
    , sum(txn) over (partition by address order by day asc) as unilp_bal
    , rank() over (partition by address order by day desc)
from unilp_transfers
)
              --end of checking current UNI INDEX|ETH LP token Balance for all wallet
              -- checking current Sushi INDEX|ETH LP token Balance for all wallet
, slp_transfers as (
    --ERC20 Tokens
    select
        date_trunc('day', evt_block_time) as day,
        "to" as address,
        sum(value/1e18) as txn
 --       sum(value/1e18) over (partition by "to" order by evt_block_time asc)AS amount
    from erc20."ERC20_evt_Transfer"
    where "contract_address" = '\xA73DF646512C82550C2b3C0324c4EEdEE53b400C'
    and evt_block_time >= '2020-10-06 00:00' -- contract creation date
    group by 1,2
    
    union all

    select
        date_trunc('day', evt_block_time) as day,
        "from" as address,
        sum(-value/1e18) as txn
  --      sum(-value/1e18) over (partition by "from" order by evt_block_time asc)AS amount
    from erc20."ERC20_evt_Transfer"
    where "contract_address" = '\xA73DF646512C82550C2b3C0324c4EEdEE53b400C'
    and evt_block_time >= '2020-10-06 00:00'  -- contract creation date
    group by 1,2
)

, slp_token_balance as (
select  *
    , sum(txn) over (partition by address order by day asc) as slp_bal
    , rank() over (partition by address order by day desc)
from slp_transfers
)              
              
              --end of checking current Sushi INDEX|ETH LP token Balance for all wallet              
             -- total number of INDEX|ETH UniLP Token, INDEX in LP contract, INDEX per LP
, unilp_net as (
select
    date_trunc('day',evt_block_time) as day
    ,sum(value/1e18) as balance
from erc20."ERC20_evt_Transfer"
where contract_address = '\x3452a7f30a712e415a0674c0341d44ee9d9786f9' ------ minted token uniswap pool contract address
and "from" = '\x0000000000000000000000000000000000000000'
group by 1

union all

select
    date_trunc('day',evt_block_time) as day
    ,sum(-value/1e18) as balance
from erc20."ERC20_evt_Transfer"
where contract_address = '\x3452a7f30a712e415a0674c0341d44ee9d9786f9' ------ burned token uniswap pool contract address
and "to" = '\x0000000000000000000000000000000000000000'
group by 1
)

, running_unilp as (
select day
--sum(balance)as index_unilp,
, sum(balance) over (order by day asc) as current_unilp
, rank() over (order by day desc)
from unilp_net
order by 1 desc
limit 1
)


, index_reserves_univ2 AS ( 
     SELECT day, rank() over (order by day desc),
            latest_reserves[3]/1e18 AS "INDEX amount",
            latest_reserves[4]/1e18 AS "ETH amount"
            
   FROM
     (SELECT date_trunc('day', evt_block_time) AS day,
     (SELECT MAX(ARRAY[evt_block_number, evt_index, reserve0, reserve1])) AS latest_reserves
         FROM uniswap_v2."Pair_evt_Sync"
         WHERE contract_address = '\x3452a7f30a712e415a0674c0341d44ee9d9786f9'
         AND date_trunc('day', evt_block_time) >= now()- interval '1 day'
      GROUP BY 1) AS day_reserves 
)

, uni_lp_summary as (
select rank() over (order by ir.day), ru.current_unilp, ir."INDEX amount" as "INDEX amount in UNI", ir."INDEX amount"/ru.current_unilp as index_per_lp
from running_unilp ru
left join index_reserves_univ2 ir
on ru.rank = ir.rank
)
                   -- end of total number of INDEX|ETH UniLP Token, INDEX in LP contract, INDEX per LP
             -- total number of INDEX|ETH SLP Token, INDEX in LP contract, INDEX per LP
, slp_net as (
select
    date_trunc('day',evt_block_time) as day
    ,sum(value/1e18) as balance
from erc20."ERC20_evt_Transfer"
where contract_address = '\xA73DF646512C82550C2b3C0324c4EEdEE53b400C' ------ minted token sushi pool contract address
and "from" = '\x0000000000000000000000000000000000000000'
group by 1

union all

select
    date_trunc('day',evt_block_time) as day
    ,sum(-value/1e18) as balance
from erc20."ERC20_evt_Transfer"
where contract_address = '\xA73DF646512C82550C2b3C0324c4EEdEE53b400C' ------ burned token sushi pool contract address
and "to" = '\x0000000000000000000000000000000000000000'
group by 1
)

, running_slp as (
select day
-- sum(balance) as index_slp
, sum(balance) over (order by day asc) as current_slp
, rank() over (order by day desc)
from slp_net
order by 1 desc
limit 1
)


, index_reserves_sushi AS ( 
     SELECT day, rank() over (order by day desc),
            latest_reserves[3]/1e18 AS "INDEX amount",
            latest_reserves[4]/1e18 AS "ETH amount"
            
   FROM
     (SELECT date_trunc('day', evt_block_time) AS day,
     (SELECT MAX(ARRAY[evt_block_number, evt_index, reserve0, reserve1])) AS latest_reserves
         FROM sushi."Pair_evt_Sync"
         WHERE contract_address = '\xA73DF646512C82550C2b3C0324c4EEdEE53b400C'
         AND date_trunc('day', evt_block_time) >= now()- interval '1 day'
      GROUP BY 1) AS day_reserves 
)

, sushi_lp_summary as (
select rank() over (order by ir.day), rs.current_slp, ir."INDEX amount" as "INDEX amount in SLP", ir."INDEX amount"/rs.current_slp as index_per_lp
from running_slp rs
left join index_reserves_sushi ir
on rs.rank = ir.rank
)
                   -- end of total number of INDEX|ETH SLP Token, INDEX in LP contract, INDEX per LP                   
                   
                   
, voting_from_unilp_temp as (
select distinct ut.address, ut.unilp_bal as unilp_bal, (ut.unilp_bal * ul.index_per_lp) as unilp_votingpow, index_per_lp
from uni_lp_summary ul
left join unilp_token_balance ut
on ul.rank = ut.rank
)
 
, voting_from_slp_temp as (
select distinct st.address, st.slp_bal as slp_bal, (st.slp_bal * sl.index_per_lp) as slp_votingpow
from sushi_lp_summary sl
left join slp_token_balance st
on sl.rank = st.rank
) 


                               -- checking added/removed INDEX|ETH SLP from/to MasterChef 
, masterchef_net_temp as (                                    
select "user", sum(amount/1e18) as amount  from sushi."MasterChef_evt_Deposit"
where pid = 75 --- 75 is used by INDEX/ETH SLP
group by 1                                  


UNION ALL

select "user", -sum(amount/1e18) as amount from sushi."MasterChef_evt_Withdraw"
where pid = 75 --- 75 is used by INDEX/ETH SLP
group by 1       

)     
                                -- net_change of INDEX|ETH LP from MasterChef
, masterchef_net as (
select "user", sum(amount) as amount 
from masterchef_net_temp
group by 1      
)

                                        -- checking wallet with SLP and Masterchef stakers
, wallet_and_masterchef_slp as (
select distinct st.address, st.slp_bal as slp_wallet, coalesce(mn.amount, 0) as slp_staked, rank() over (partition by address order by st.day desc)
--, sum(mn.amount) over (order by mn."user")
from  slp_token_balance st
left join masterchef_net mn
on st.address = mn."user"
where st.address != '\xc2edad668740f1aa35e4d8f227fb8e17dca888cd'-- (SushiSwap: MasterChef LP Staking Pool) contract address (cannot vote)
and st.address != '\x95c69c3220b31b843f1cf20bee5c53fcde7fc12e'-- contract address (cannot vote)
and st.address != '\x280ac711bb99de7c73fb70fb6de29846d5e4207f'-- contract address (cannot vote)
and st.address != '\xe11fc0b43ab98eb91e9836129d1ee7c3bc95df50'--  SushiSwap: SushiMaker contract address (cannot vote)
and st.address != '\x88ad09518695c6c3712ac10a214be5109a655671'-- POA Network: xDAI OmniBridge contract address (cannot vote)
and st.rank = 1
)

, voting_from_index as (
select distinct address, index_bal 
from index_token_balance
where rank = 1
)

, voting_from_unilp as (
select address, unilp_bal as unilp_bal, index_per_lp as "Index/LP(Uni)"
from voting_from_unilp_temp
)

, voting_from_slp as (
select wm.address, wm.slp_wallet as slp_bal, wm.slp_staked, sl.index_per_lp as "Index/LP(Sushi)", 
((wm.slp_wallet + wm.slp_staked)*sl.index_per_lp)  as index_votingpow_sushi
from wallet_and_masterchef_slp wm
left join sushi_lp_summary sl
on wm.rank = sl.rank
)

, alladdress as (
select address from voting_from_index
union
select address from voting_from_unilp
union
select address from voting_from_slp

)

, summary_all as (
select aa.address, fi.index_bal
, coalesce(fu.unilp_bal, 0) as unilp_bal, fu."Index/LP(Uni)", (fu.unilp_bal * fu."Index/LP(Uni)") as unilp_votingpow
, coalesce(fs.slp_bal, 0) as slp_bal, coalesce(fs.slp_staked, 0) as slp_staked , fs."Index/LP(Sushi)", ((fs.slp_bal + fs.slp_staked) * fs."Index/LP(Sushi)") as slp_votingpow
from alladdress aa
left join voting_from_index fi
on aa.address = fi.address
left join voting_from_slp fs
on aa.address = fs.address
left join voting_from_unilp fu
on aa.address = fu.address
)
 
 
select s.address, s.index_bal, s.unilp_bal, coalesce(s.unilp_votingpow, 0) as unilp_votingpow, s.slp_bal, s.slp_staked, coalesce(s.slp_votingpow, 0) as slp_votingpow,
s.index_bal + coalesce(s.unilp_votingpow, 0) + coalesce(s.slp_votingpow, 0) as totalvoting,
case when nv.wallet_address is not null then 'Nonvoting' else 'Voting' end as wallet_type
from summary_all s
left join dune_user_generated.index_nonvoting_addresses nv on s.address = nv.wallet_address
where s.address <> '\x0000000000000000000000000000000000000000'
