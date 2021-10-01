--https://dune.xyz/queries/124590
create or replace function dune_user_generated.get_lp_positions_v2(
pair bytea,
token bytea,
types text,
lp_mint_addr bytea,
lp_decimal numeric,
token_decimal numeric,
times timestamp)
returns table (holder bytea, types text, lp_position numeric) as 

$body$

with lps as (
select
    sum(amount) as total_lp_token
from (
    select 
        "value"/$5 as amount
    from erc20."ERC20_evt_Transfer"
    where "contract_address" = $1
      and "from" = $4
      and "evt_block_time" < $7
    
    union all 
    
    select 
        -"value"/$5 as amount
    from erc20."ERC20_evt_Transfer"
    where "contract_address" = $1
      and "to" = $4
      and "evt_block_time" < $7
  
      ) x

 ),
 dpi_amount_in_pair as 
 (
select
    sum(amount) as total_dpi_amount
from (
    select
        -"value"/$6 as amount
    from erc20."ERC20_evt_Transfer"
    where "contract_address" = $2
      and "from" = $1
      and "evt_block_time" < $7
      
    union all 
    
    select 
        "value"/$6 as amount
    from erc20."ERC20_evt_Transfer"
    where "contract_address" = $2
      and "to" = $1
      and "evt_block_time" < $7
      ) x
),
-- select * from dpi_amount_in_pair
user_lps as
(
select
    holder,
    sum(amount) as lp_token
from (
    select 
        "from" as holder,
        -"value"/$5 as amount
    from erc20."ERC20_evt_Transfer"
    where "contract_address" = $1
    and "evt_block_time" < $7
    
    union all 
    
    select 
        "to" as holder,
        "value"/$5 as amount
    from erc20."ERC20_evt_Transfer"
    where "contract_address" = $1
    and "evt_block_time" < $7
      ) x
 where  holder  != $4
 group by holder


)
-- select * from user_lps
select
    holder,
    --lp_token,
    --total_lp_token,
    --total_dpi_amount,
    $3 as types,
    (lp_token/total_lp_token)*total_dpi_amount as lp_position
from (select * from user_lps where lp_token > 0.01 ) a
cross join lps
cross join dpi_amount_in_pair;

$body$

language sql;

------------------------------------------------------------
-- uni v2
-- 0x4d5ef58aac27d99935e5b6b4a6778ff292059991 pair
-- 0x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b dpi
-- 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 weth
-- 0x4d5ef58aac27d99935e5b6b4a6778ff292059991 lp token

-- sushi dpi/eth
-- 0x34b13f8cd184f55d0bd4dd1fe6c07d46f245c7ed pair
-- 0x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b dpi

-- cream staking
-- 0x2A537Fa9FFaea8C1A41D3C2B68a9cb791529366D pool
-- 0x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b dpi
-- lp decimal 8
-- mint 
-- sushi
with dpi_in_lp_staking as
(
select
    holder,
    sum(lp_position) as amount
from (
    select 
       * 
    from dune_user_generated.get_lp_positions_v2(
    '\x34b13f8cd184f55d0bd4dd1fe6c07d46f245c7ed'::bytea, 
    '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'::bytea,
    'sushi_lp',
    '\x0000000000000000000000000000000000000000'::bytea,
    1e18,
    1e18,
    '2021-04-13 00:00'::timestamp)
    
    union all 
    -- uni v2 
    select 
       * 
    from dune_user_generated.get_lp_positions_v2(
    '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'::bytea, 
    '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'::bytea,
    'uni_v2_lp',
    '\x0000000000000000000000000000000000000000'::bytea,
    1e18,
    1e18,
    '2021-04-13 00:00'::timestamp)
    
    union all
    -- cream 
    select 
    *
    --   sum(lp_position) 
    from dune_user_generated.get_lp_positions_v2(
    '\x2A537Fa9FFaea8C1A41D3C2B68a9cb791529366D'::bytea, 
    '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'::bytea,
    'cream_staking',
    '\x2A537Fa9FFaea8C1A41D3C2B68a9cb791529366D'::bytea,
    1e8,
    1e18,
    '2021-04-13 00:00'::timestamp)
   ) x
group by holder
),

 dpi_in_wallet as
(
select
    holder,
    sum(amount) as amount
from (
    select 
        "to" as holder,
        "value"/1e18 as amount,
        "evt_tx_hash",
        "evt_block_time"
    from erc20."ERC20_evt_Transfer"
    where  "contract_address"='\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    and "evt_block_time" < '2021-04-13 00:00:00' 
    
    union all
    
    select 
        "from" as holder,
        -"value"/1e18 as amount,
        "evt_tx_hash",
        "evt_block_time"
    from erc20."ERC20_evt_Transfer"
    where  "contract_address"='\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    and "evt_block_time" < '2021-04-13 00:00:00' 
    ) x
group by holder
)
,

promotion_users as
(
    select 
         "to" as holder,
         sum("value"/1e18)::int as bought_amount
    from erc20."ERC20_evt_Transfer"
    where --"to" in (select address from promotion_rewards) and
    "from" = '\xd291328a6c202c5b18dcb24f279f69de1e065f70'
    and "contract_address"='\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    and "evt_block_time" >= '2021-04-13 00:00:00' 
    and "evt_block_time" <= '2021-04-22 00:00:00'
    group by "to"
),

dpi_balance_before_promo as
(
select
    a.holder,
    (coalesce(b.amount, 0) + coalesce(c.amount, 0))::int as total_dpi_amount,
    coalesce(b.amount, 0)::int as dpi_in_wallet,
    coalesce(c.amount, 0)::int as dpi_lp_stake,
    bought_amount
from promotion_users a
left join dpi_in_wallet b
on a.holder = b.holder
left join dpi_in_lp_staking c
on a.holder = c.holder
)

select 
    * ,
    sum(case when total_dpi_amount >= 250 then 1 else 0 end ) over() as big_fish_number,
    sum(case when total_dpi_amount < 250 and total_dpi_amount > 0  then 1 else 0 end ) over() as small_fish_number,
    sum(case when total_dpi_amount = 0 then 1 else 0 end ) over() as newbies_number,
    sum(case when total_dpi_amount = 0 then bought_amount else 0 end ) over() as newbies_bought_amount
from dpi_balance_before_promo
order by total_dpi_amount desc;

