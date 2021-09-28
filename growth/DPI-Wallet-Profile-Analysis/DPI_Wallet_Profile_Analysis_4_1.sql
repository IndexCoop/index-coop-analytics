--https://dune.xyz/queries/124678

/* lp function */
create or replace function dune_user_generated.get_lp_positions(
pair bytea,
token bytea,
types text,
lp_mint_addr bytea,
lp_decimal numeric,
token_decimal numeric )
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
    
    union all 
    
    select 
        -"value"/$5 as amount
    from erc20."ERC20_evt_Transfer"
    where "contract_address" = $1
      and "to" = $4
  
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
    
    union all 
    
    select 
        "value"/$6 as amount
    from erc20."ERC20_evt_Transfer"
    where "contract_address" = $2
      and "to" = $1
  
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
    
    union all 
    
    select 
        "to" as holder,
        "value"/$5 as amount
    from erc20."ERC20_evt_Transfer"
    where "contract_address" = $1
  
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

-- uni v3 dpi/eth
-- 0x9359c87b38dd25192c5f2b07b351ac91c90e6ca7 pair
-- 0x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b dpi

drop table if exists dune_user_generated.dpi_lp_position;
create table  dune_user_generated.dpi_lp_position as 
with uniswapv3_pool as (
  select
      pool,
      token0,
      tok0.symbol as symbol0,
      tok0.decimals as decimals0,
      token1,
      tok1.symbol as symbol1,
      tok1.decimals as decimals1

  from        uniswap_v3."Factory_evt_PoolCreated" pool
  
  inner join  erc20."tokens" tok0 
  on          pool.token0 = tok0.contract_address
  
  inner join  erc20."tokens" tok1 
  on          pool.token1 = tok1.contract_address

  where       pool = '\x9359c87b38dd25192c5f2b07b351ac91c90e6ca7'
)
,
-- select * from uniswapv3_pool
v3_position_mint as
(
select 
    "evt_tx_hash",
    "contract_address",
    "tickLower",
    "tickUpper"
from uniswap_v3."Pair_evt_Mint"
where "contract_address" = '\x9359c87b38dd25192c5f2b07b351ac91c90e6ca7'

)
,
-- select count(*),count(distinct evt_tx_hash) from v3_position_mint;

v3_position_nfts_id as
(
select
    distinct
    "tokenId",
    b."contract_address",
    "tickLower",
    "tickUpper"
from uniswap_v3."NonfungibleTokenPositionManager_evt_IncreaseLiquidity" a 
inner join  v3_position_mint b
on a.evt_tx_hash = b.evt_tx_hash
)
,
-- select count(*),count(distinct "tokenId") from v3_position_nfts_id;
v3_positions as
(
select
    "tokenId",
    sum("liquidity") as current_liquidity
from (
    select 
        "tokenId",
        -"liquidity" as liquidity
    from uniswap_v3."NonfungibleTokenPositionManager_evt_DecreaseLiquidity"
    where "tokenId" in (select "tokenId" from v3_position_nfts_id)
    
    union all 
    
    select 
        "tokenId",
        "liquidity"
    from uniswap_v3."NonfungibleTokenPositionManager_evt_IncreaseLiquidity"
    where "tokenId" in (select "tokenId" from v3_position_nfts_id)
    ) x
    group by "tokenId"
),
-- select * from v3_positions;
v3_positions_detail as
(
select
  a."tokenId",
  a.current_liquidity,
  b."tickLower",
  b."tickUpper",
  token0,
  symbol0,
  decimals0,
  token1,
  symbol1,
  decimals1,
    CASE 
        WHEN "tickLower" < 0 THEN (1.0001^("tickLower")) * (10^(ABS(decimals0-decimals1)))
        ELSE (1.0001^("tickLower")) / (10^(ABS(decimals0-decimals1)))
    END AS price_lower,
    CASE 
        WHEN "tickUpper" < 0 THEN (1.0001^("tickUpper")) * (10^(ABS(decimals0-decimals1)))
        ELSE (1.0001^("tickUpper")) / (10^(ABS(decimals0-decimals1)))
    END AS price_upper
from v3_positions a 
left join v3_position_nfts_id b
on a."tokenId" = b."tokenId"
left join uniswapv3_pool c
on b.contract_address = c.pool
where current_liquidity != 0
)
,
-- select * from v3_positions_detail;
-- select * from uniswap_v3."NonfungibleTokenPositionManager_evt_DecreaseLiquidity" limit 10;

price_feed as (
select  
    -- "sqrtPriceX96",
    -- (power("sqrtPriceX96",2)/ (2^(96*2))) as price
    avg((power("sqrtPriceX96",2)/ (2^(96*2)))) as price
from ( 
    select * from  uniswap_v3."Pair_evt_Swap" 
    where contract_address ='\x9359c87b38dd25192c5f2b07b351ac91c90e6ca7'
    order by "evt_block_time" desc
    limit 3
   ) x
),
-- select * from price_feed;

v3_position_real_temp1 as (
select      
    a.*,
    price,
    case
        when price between price_lower and price_upper then 'between'
        when price < price_lower then 'below'
        when price > price_upper then 'above'
    end as price_wrt_range,
    current_liquidity/sqrt(price_lower) as amount0_lp,
    current_liquidity*sqrt(price_lower) as amount1_lp,
    current_liquidity/sqrt(price_upper) as amount0_up,
    current_liquidity*sqrt(price_upper) as amount1_up,
    current_liquidity/sqrt(price) as amount0_cp,
    current_liquidity*sqrt(price) as amount1_cp

from  v3_positions_detail a 
cross join price_feed b
),

v3_position_real_temp2 as (
select
    a.*,
    case
        when price_wrt_range = 'between' then (amount0_cp - amount0_up)/10^decimals0
        when price_wrt_range = 'above' then 0
        when price_wrt_range = 'below' then (amount0_lp - amount0_up)/10^decimals0
    end as amount0_real,
    case
        when price_wrt_range = 'between' then (amount1_cp - amount1_lp)/10^decimals1
        when price_wrt_range = 'above' then (amount1_up - amount1_lp)/10^decimals1
        when price_wrt_range = 'below' then 0
    end as amount1_real

from v3_position_real_temp1 a 
),

v3_position_real as (
select      
    a.*,
    case
        when symbol0 = 'DPI' then amount0_real
        when symbol1 = 'DPI' then amount1_real
    end as current_dpi_amount

from v3_position_real_temp2 as a
)
,
-- select count(*),count(distinct "tokenId")  from v3_position_real;
-- select distinct * from v3_position_real;
-- toekn id  owner
position_nft_owner as
(
select
    holder,
    "tokenId"
from (
    select
        "to" as holder,
        "tokenId",
        "evt_block_time",
        row_number() over( partition by "tokenId" order by "evt_block_time" desc ) as rnk
    from erc721."ERC721_evt_Transfer" 
    where "contract_address" = '\xc36442b4a4522e871399cd717abdd847ab11fe88'
    and "tokenId" in (select "tokenId" from v3_positions_detail)
        ) x
    where rnk = 1
)

select
    holder,
    types,
    lp_position::int as lp_position
from (
    -- uni v2 
    select 
       * 
    from dune_user_generated.get_lp_positions(
    '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'::bytea, 
    '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'::bytea,
    'uni_v2_lp',
    '\x0000000000000000000000000000000000000000'::bytea,
    1e18,
    1e18 )
    
    union all
    -- sushi lp
    select 
       * 
    from dune_user_generated.get_lp_positions(
    '\x34b13f8cd184f55d0bd4dd1fe6c07d46f245c7ed'::bytea, 
    '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'::bytea,
    'sushi_lp',
    '\x0000000000000000000000000000000000000000'::bytea,
    1e18,
    1e18 )
    union all
    
    select
         holder,
         'uni_v3_lp' as types,
         sum(current_dpi_amount) as lp_position
    from position_nft_owner a
    left join v3_position_real b
    on a."tokenId" = b."tokenId"
    group by holder
     ) x
where lp_position >= 1
;


with lp_position as
(
SELECT 
    holder,
    coalesce(sushi_lp, 0) as sushi_lp,
     coalesce(uni_v2_lp, 0) as uni_v2_lp,
     coalesce(uni_v3_lp, 0) as uni_v3_lp
FROM crosstab ( 
'SELECT holder, types, lp_position FROM dune_user_generated.dpi_lp_position ORDER BY 1, 2',
'SELECT DISTINCT types FROM dune_user_generated.dpi_lp_position ORDER BY 1' 
) AS ( holder bytea, sushi_lp numeric, uni_v2_lp numeric, uni_v3_lp numeric )
),
wallet_balance as
(
select
    holder,
    dpi_balance::int as wallet
from dune_user_generated.dpi_balance_by_day
where rnk = 1 
)

select
    count(*) as number_of_lps,
    sum(dpi_balance) as total_dpi_balance,
    sum(dpi_in_wallet) as total_dpi_in_wallet,
    sum(dpi_in_lp) as total_dpi_in_lp,
    sum(sushi_lp) as total_dpi_in_sushi_lp,
    sum(uni_v2_lp) as total_dpi_in_uni_v2_lp,
    sum(uni_v3_lp) as total_dpi_in_uni_v3_lp
from (
    select
        -- count(*),
        -- count(distinct a.holder)
        a.holder,
        (coalesce(wallet, 0) + sushi_lp + uni_v2_lp + uni_v3_lp) as dpi_balance,
        coalesce(wallet, 0) as dpi_in_wallet, 
        ( sushi_lp + uni_v2_lp + uni_v3_lp) as dpi_in_lp,
        sushi_lp,
        uni_v2_lp,
        uni_v3_lp
    from lp_position a 
    left join wallet_balance b
    on a.holder = b.holder 
   ) x
;

