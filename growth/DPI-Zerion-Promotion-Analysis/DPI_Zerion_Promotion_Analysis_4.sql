--https://dune.xyz/queries/131117
/* DPI */
with token_acquire_promotion as
(
    select 
         "to" as holder,
         sum("value"/1e18)::int as bought_amount
    from erc20."ERC20_evt_Transfer"
    where --"to" in (select address from promotion_rewards) and
    "from" = '\xd291328a6c202c5b18dcb24f279f69de1e065f70'
    and "contract_address"='\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    and "evt_block_time" >= '2021-04-13 00:00:00' 
    and "evt_block_time" < '2021-04-22 00:00:00'
    group by "to"
)
,
-- select * from promotion_users_bought;

token_transfer as 
(
    select
        holder,
        day_time,
        tx_hash,
        trans_out,
        sum(trans_out) over( partition by holder order by day_time rows between unbounded preceding and current row) as cumurative_out
    from (
    select 
         "from" as holder,
         "evt_block_time" as day_time,
         "evt_tx_hash" as tx_hash,
        --  date_trunc('day',"evt_block_time") as days,
        ("value"/1e18)::int as trans_out
    from erc20."ERC20_evt_Transfer"
    where  "contract_address"='\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    and "evt_block_time" >= '2021-04-22 00:00:00'
    and "from" in (select holder from token_acquire_promotion) 
        ) x
    order by holder, day_time 
)
,
-- select * from dpi_transfer;
trans_out_by_day_temp1 as
(
select
    * 
from ( 
    select
        a.holder,
        day_time,
        tx_hash,
        bought_amount,
        trans_out,
        cumurative_out,
        case when cumurative_out <= bought_amount then trans_out
             else GREATEST(bought_amount-cumurative_out+trans_out,0)
        end as trans_out_real
    from token_transfer a 
    left join token_acquire_promotion b
    on a.holder = b.holder
   ) x
where trans_out_real != 0
)
-- select 
-- *
-- -- count(*),count(distinct tx_hash),sum(trans_out_real) 
-- from trans_out_by_day_temp1
-- order by tx_hash;
,
token_trade as 
(
-- sushi trade
select 
    "evt_tx_hash" as tx_hash
from 
sushi."Pair_evt_Swap" 
where "contract_address" in (
'\x34b13f8cd184f55d0bd4dd1fe6c07d46f245c7ed',
'\x8775aE5e83BC5D926b6277579c2B0d40c7D9b528'
    )
and "evt_block_time" >= '2021-04-22 00:00:00'

union all 
-- uni_v2
select 
    "evt_tx_hash" as tx_hash
from 
uniswap_v2."Pair_evt_Swap"
where "contract_address" in (
'\x4d5ef58aAc27d99935E5b6B4A6778ff292059991'
    )
and "evt_block_time" >= '2021-04-22 00:00:00'

union all
-- uni_v3
select 
    "evt_tx_hash" as tx_hash
from 
uniswap_v3."Pair_evt_Swap"
where "contract_address" in (
'\x9359c87B38DD25192c5f2b07b351ac91C90E6ca7'
    )
and "evt_block_time" >= '2021-04-22 00:00:00'

),

token_staking as
(
select 
"evt_tx_hash" as tx_hash
from creamfinance."CErc20Delegate_evt_Mint" 
where "contract_address" in (
'\x2a537fa9ffaea8c1a41d3c2b68a9cb791529366d'
)
and "evt_block_time" >= '2021-04-22 00:00:00'

),

token_lp as
(


-- sushi trade
select 
    "evt_tx_hash" as tx_hash
from 
sushi."Pair_evt_Mint" 
where "contract_address" in (
'\x34b13f8cd184f55d0bd4dd1fe6c07d46f245c7ed',
'\x8775aE5e83BC5D926b6277579c2B0d40c7D9b528'
    )
and "evt_block_time" >= '2021-04-22 00:00:00'

union all 
-- uni_v2
select 
    "evt_tx_hash" as tx_hash
from 
uniswap_v2."Pair_evt_Mint"
where "contract_address" in (
'\x4d5ef58aAc27d99935E5b6B4A6778ff292059991'
    )
and "evt_block_time" >= '2021-04-22 00:00:00'

union all
-- uni_v3
select 
    "evt_tx_hash" as tx_hash
from 
uniswap_v3."Pair_evt_Mint"
where "contract_address" in (
'\x9359c87B38DD25192c5f2b07b351ac91C90E6ca7'
    )
and "evt_block_time" >= '2021-04-22 00:00:00'
)

select
    sold_amount,
    staked_amount,
    lp_amount,
    total_amount,
    (total_amount-sold_amount-staked_amount-lp_amount) as other_amount
from (   
select 
    -- holder,
    -- day_time,
    -- a.tx_hash,
    -- bought_amount,
    -- trans_out,
    -- trans_out_real,
    -- b.tx_hash as swap,
    -- c.tx_hash as staking,
    -- d.tx_hash as lp
    sum(case when b.tx_hash is not null then trans_out_real else 0 end) as sold_amount,
    sum(case when c.tx_hash is not null then trans_out_real else 0 end) as staked_amount,
    sum(case when d.tx_hash is not null then trans_out_real else 0 end) as lp_amount,
    sum(trans_out_real) as total_amount

from trans_out_by_day_temp1 a
left join (select distinct * from token_trade) b
on a.tx_hash = b.tx_hash
left join token_staking c
on a.tx_hash = c.tx_hash
left join token_lp d
on a.tx_hash = d.tx_hash 
) x;

-- select * from trans_out_by_day_temp1;
