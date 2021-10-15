--https://dune.xyz/queries/117984

with token_balances as (
  select b.wallet_address
    , coalesce(nv.wallet_type, 'Voting') as wallet_type
    , b.amount
  from erc20."view_token_balances_latest" b
  left join dune_user_generated.index_addresses nv on b.wallet_address = nv.wallet_address
  where b.token_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab' -- INDEX token address
  and b.wallet_address <> '\x0000000000000000000000000000000000000000'
  and amount > 0
)
select b.wallet_address
    , b.amount as INDEX_held
  , row_number() over (order by amount desc) as wallet_rank
  , sum(b.amount) over () as total_voting_index
  , b.amount / sum(b.amount) over () as pct_total
  , sum(b.amount) over (order by amount desc rows between unbounded preceding and current row) / 
    sum(b.amount) over () as cum_pct
from token_balances b
where wallet_type = 'Voting'
order by amount desc