--https://dune.xyz/queries/185266
/* trading*/
with promotion_reward_address as
(
select 
    distinct "to" as airdrop_address
    -- "value"/1e18 as index_amount,
    -- sum("value"/1e18) total_index_airdropped
from erc20."ERC20_evt_Transfer"
where "evt_tx_hash" in (
'\x005b48fa78bd822f630e3bac1322e48395328d9409276357b25cc1e2ae9ac184',
'\x145190b126f2a44ccc5112a615db021b617d55f4893ccd559b34ddc04180d8ac',
'\xaf14e7447800cf61df7b88d4ad4362c234685b4882332a702a8549fb3b0cb026'
)
and "from" != '\x69238af5756617e5218810057a03da509ec51fd4'
),

token_in_tx_hash as
(
select
    "evt_tx_hash",
    "contract_address",
    "to" as address,
    "value"/1e18 as amount
from erc20."ERC20_evt_Transfer"
where "contract_address" in
( 
-- dpi
'\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b',
-- index
'\x0954906da0Bf32d5479e25f46056d22f08464cab',
-- eth 2x
'\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd',
-- mvi
'\x72e364f2abdc788b7e918bc238b21f109cd634d7'
)
and "to" in (select * from promotion_reward_address)
),
token_contracts as 
(
select 
    distinct "contract_address"
from token_in_tx_hash
),

uni_pairs_v2 AS 
( -- Get exchange contract address and "other token" for WETH
    SELECT cr."pair" AS contract
    FROM uniswap_v2."Factory_evt_PairCreated" cr
    WHERE token0 in (select * from token_contracts ) OR  token1 in (select * from token_contracts )
),
uni_pairs_v3 AS
(
    SELECT cr."pool" AS contract
    FROM uniswap_v3."Factory_evt_PoolCreated" cr
    WHERE token0 in (select * from token_contracts ) OR  token1 in (select * from token_contracts )
),
sushi_pairs as
(
 SELECT cr."pair" AS contract
    FROM sushi."Factory_evt_PairCreated" cr
    WHERE token0 in (select * from token_contracts ) OR  token1 in (select * from token_contracts )
),


token_trade as 
(
-- sushi trade
select 
    "evt_tx_hash" as tx_hash
from 
sushi."Pair_evt_Swap" 
where "contract_address" in (select * from sushi_pairs)
and "evt_block_time" >= '2021-04-22 00:00:00'

union all 
-- uni_v2
select 
    "evt_tx_hash" as tx_hash
from 
uniswap_v2."Pair_evt_Swap"
where  "contract_address" in (select * from uni_pairs_v2)
and "evt_block_time" >= '2021-04-22 00:00:00'

union all
-- uni_v3
select 
    "evt_tx_hash" as tx_hash
from uniswap_v3."Pair_evt_Swap"
where  "contract_address" in (select * from uni_pairs_v3)
and "evt_block_time" >= '2021-04-22 00:00:00'
),

-- select "evt_tx_hash" from token_in_tx_hash
-- where "evt_tx_hash" in (select * from token_trade)
-- group by "evt_tx_hash" 
-- having  count(*) > 1;
trade_tx as 
(
select 
    distinct *
    from token_in_tx_hash
where "evt_tx_hash" in (select * from token_trade)
),

result_sum  as 
(
select
    contract_address,
    count(distinct address) as bought_more_address,
    sum(amount) as amount
from trade_tx
group by contract_address
)

select
    symbol,
    (select count(distinct address) from trade_tx) as total_number_of_address_bought_more_after_promotion,
    bought_more_address,
    amount
from result_sum a
left join erc20."tokens" b
on a.contract_address = b."contract_address"
order by symbol;

-- select * from trade_tx where "evt_tx_hash" ='\x0628e43879c9304e09d822f61e6832b1e31048c018d1793b0c3f8f1486f329a9';


