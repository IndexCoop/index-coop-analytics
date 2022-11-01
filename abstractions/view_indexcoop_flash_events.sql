-- https://dune.com/queries/1502333
-- Flash Mint & Redeem Events

create or replace view dune_user_generated.indexcoop_flash_events as 

select
    'Issue' as event,
    'v1' as version,
    "evt_block_time" as block_time,
    e."_recipient" as recipient,
    t.symbol,
    "_inputToken" as input_token,
    "_amountInputToken" as input_token_amount,
    "_setToken" as output_token,
    "_amountSetIssued" as output_token_amount
from    setprotocol_v2."ExchangeIssuance_evt_ExchangeIssue" e inner join dune_user_generated.indexcoop_tokens t on t.token_address = e."_setToken"
  
union

select
    'Redeem' as event,
    'v1' as version,
    "evt_block_time" as block_time,
    e."_recipient" as recipient,
    t.symbol,
    "_setToken" as input_token,
    "_amountSetRedeemed" as input_token_amount,
    "_outputToken" as output_token,
    "_amountOutputToken" as output_token_amount
from    setprotocol_v2."ExchangeIssuance_evt_ExchangeRedeem" e inner join dune_user_generated.indexcoop_tokens t on t.token_address = e."_setToken"

union 

select
    'Issue' as event,
    '0x' as version,
    "evt_block_time" as block_time,
    e."_recipient" as recipient,
    t.symbol,
    "_inputToken" as input_token,
    "_amountInputToken" as input_token_amount,
    "_setToken" as output_token,
    "_amountSetIssued" as output_token_amount
from    setprotocol_v2."ExchangeIssuanceZeroEx_evt_ExchangeIssue" e inner join dune_user_generated.indexcoop_tokens t on t.token_address = e."_setToken"

union

select
    'Redeem' as event,
    '0x' as version,
    "evt_block_time" as block_time,
    e."_recipient" as recipient,
    t.symbol,
    "_setToken" as input_token,
    "_amountSetRedeemed" as input_token_amount,
    "_outputToken" as output_token,
    "_amountOutputToken" as output_token_amount
from    setprotocol_v2."ExchangeIssuanceZeroEx_evt_ExchangeRedeem" e inner join dune_user_generated.indexcoop_tokens t on t.token_address = e."_setToken"

union

select
    'Issue' as event,
    'Leveraged' as version,
    "evt_block_time" as block_time,
    e."_recipient" as recipient,
    t.symbol,
    "_inputToken" as input_token,
    "_amountInputToken" as input_token_amount,
    "_setToken" as output_token,
    "_amountSetIssued" as output_token_amount
from    indexcoop."ExchangeIssuanceLeveraged_evt_ExchangeIssue" e inner join dune_user_generated.indexcoop_tokens t on t.token_address = e."_setToken"

union

select
    'Redeem' as event,
    'Leveraged' as version,
    "evt_block_time" as block_time,
    e."_recipient" as recipient,
    t.symbol,
    "_setToken" as input_token,
    "_amountSetRedeemed" as input_token_amount,
    "_outputToken" as output_token,
    "_amountOutputToken" as output_token_amount
from    indexcoop."ExchangeIssuanceLeveraged_evt_ExchangeRedeem" e inner join dune_user_generated.indexcoop_tokens t on t.token_address = e."_setToken"
