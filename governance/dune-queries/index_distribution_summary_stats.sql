with nonvoting_addresses as (
  select '\x8f06FBA4684B5E0988F215a47775Bb611Af0F986'::bytea as wallet_address -- 1st DPI LM Rewards
  union select '\xB93b505Ed567982E2b6756177ddD23ab5745f309'::bytea -- 2nd DPI LM Rewards
  union select '\x56d68212113AC6cF012B82BD2da0E972091940Eb'::bytea -- ETHFLI LM Rewards (not active yet)
  union select '\x5bC4249641B4bf4E37EF513F3Fa5C63ECAB34881'::bytea -- 2nd MVI LM Rewards
  union select '\xa73df646512c82550c2b3c0324c4eedee53b400c'::bytea -- INDEX on Sushiswap
  union select '\x3452a7f30a712e415a0674c0341d44ee9d9786f9'::bytea -- INDEX on Uniswap_v2
  union select '\x8c13148228765Ba9E84EAf940b0416a5e349A5e7'::bytea -- INDEX on Uniswap_v3 
  union select '\xd3d555bb655acba9452bfc6d7cea8cc7b3628c55'::bytea --- Set: rebalancer TBD if this needs to be included
  union select '\x9467cfadc9de245010df95ec6a585a506a8ad5fc'::bytea -- INDEX Treasury
  union select '\x66a7d781828b03ee1ae678cd3fe2d595ba3b6000'::bytea -- Index Methodologist Vesting
  union select '\xdd111f0fc07f4d89ed6ff96dbab19a61450b8435'::bytea -- Early Community Rewards Vesting
  union select '\x8f06fba4684b5e0988f215a47775bb611af0f986'::bytea -- To Initial Liquidity Mining Vesting
  union select '\x26e316f5b3819264df013ccf47989fb8c891b088'::bytea -- To Index Community Treasury 1 Year 
  union select '\xd89c642e52bd9c72bcc0778bcf4de307cc48e75a'::bytea -- To Index Community Treasury Year 2
  union select '\x71f2b246f270c6af49e2e514ca9f362b491fbbe1'::bytea -- To Index Community Treasury Year 3
  union select '\xf64d061106054fe63b0aca68916266182e77e9bc'::bytea -- To Set Labs Year 1 Vesting
  union select '\x4c11dfd35a4fe079b41d5d9729ed34c00d487712'::bytea -- To Set Lab Year 2 Vesting
  union select '\x0d627ca04a97219f182dab0dc2a23fb4a5b02a9d'::bytea -- To Set Lab Year 3 Vesting
  union select '\x5c29aa6761803bcfda7f683eaa0ff9bddda3649d'::bytea -- To DeFi Pulse Year 1 Vesting
  union select '\xce3c6312385fcf233ab0de574b0cb1a588566c3f'::bytea -- To DeFi Pulse Year 2 Vesting
  union select '\x0f58793e8cf39d6b60919ffaf773a7f95a568146'::bytea -- To DeFi Pulse Year 3 Vesting
    
)
, token_balances as (
  select b.wallet_address
    , case when nv.wallet_address is not null then 'Nonvoting' else 'Voting' end as wallet_type
    , b.amount
  from erc20."view_token_balances_latest" b
  left join nonvoting_addresses nv on b.wallet_address = nv.wallet_address
  where b.token_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab' -- INDEX token address
)
, wallets_ranked as (
select b.wallet_address
  , row_number() over (order by amount desc) as wallet_rank
  , b.amount / sum(b.amount) over () as pct_total
  , sum(b.amount) over (order by amount desc rows between unbounded preceding and current row) / 
    sum(b.amount) over () as cum_pct
from token_balances b
where wallet_type = 'Voting'
order by amount desc
)
select min(wallet_rank) as nakamoto_coefficient
  , 
from wallets_ranked
where cum_pct > .5
  