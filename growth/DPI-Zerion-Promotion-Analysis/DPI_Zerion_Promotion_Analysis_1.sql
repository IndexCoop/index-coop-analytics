--https://dune.xyz/queries/109213
-- Zerion Promotion Analysis
--Brief recap of promotion: 
--Between Apr 13th - Apr 21 2021 anyone buying $DPI through Zerion was airdropped 5% of their purchase in $INDEX tokens. 
--Promotion was capped at $100,000 of Index.
--reward
--https://etherscan.io/tx/0x145190b126f2a44ccc5112a615db021b617d55f4893ccd559b34ddc04180d8ac
--https://etherscan.io/tx/0xaf14e7447800cf61df7b88d4ad4362c234685b4882332a702a8549fb3b0cb026
--https://etherscan.io/tx/0x005b48fa78bd822f630e3bac1322e48395328d9409276357b25cc1e2ae9ac184
--0xa5025faba6e70b84f74e9b1113e5f7f4e7f4859f Multisender.app
--0x69238af5756617e5218810057a03da509ec51fd4 sender

with promotion_rewards as
(
select 
    count(distinct "to") as airdrop_address_count,
    -- "value"/1e18 as index_amount,
    sum("value"/1e18) total_index_airdropped
from erc20."ERC20_evt_Transfer"
where "evt_tx_hash" in (
'\x005b48fa78bd822f630e3bac1322e48395328d9409276357b25cc1e2ae9ac184',
'\x145190b126f2a44ccc5112a615db021b617d55f4893ccd559b34ddc04180d8ac',
'\xaf14e7447800cf61df7b88d4ad4362c234685b4882332a702a8549fb3b0cb026'
)
and "from" != '\x69238af5756617e5218810057a03da509ec51fd4'
),
promotion_dpi as
(
select 
    count(distinct "to") as bought_address_count,
    sum("value"/1e18) as bought_dpi_amount
from erc20."ERC20_evt_Transfer"
where --"to" in (select address from promotion_rewards) and
"from" = '\xd291328a6c202c5b18dcb24f279f69de1e065f70'
and "contract_address"='\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
and "evt_block_time" >= '2021-04-13 00:00:00' 
and "evt_block_time" <= '2021-04-22 00:00:00'
-- group by "to"
)
select
    *
from promotion_rewards a
cross join promotion_dpi;


