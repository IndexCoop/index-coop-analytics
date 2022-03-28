-- https://dune.xyz/queries/300077
-- TODO: This needs optimization.
-- Also, we should double check that BasicIssuanceModule and ExchangeIssuance events are both getting emitted
-- Are there any parsing steps that are commonly done that we can migrate into here?

CREATE OR REPLACE view dune_user_generated.indexcoop_issuance_events as 

select 
    a.evt_block_time,
    a.evt_tx_hash,
    a."_setToken" as token_address,
    b.symbol as symbol,
    case
    when c.evt_tx_hash is not null then 'Exchange'
    else 'Standard'
    end as contract_use_type,
    'Issue' as evt_type,
    a."_issuer" as wallet_address,
    a."_quantity"/1e18 as amount
from        setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued" a
inner join  dune_user_generated.indexcoop_tokens b on a."_setToken" = b.token_address
            and b.issuance_model = 'Standard'
left join   setprotocol_v2."ExchangeIssuance_evt_ExchangeIssue" c on a.evt_tx_hash = c.evt_tx_hash

union all

select 
    a.evt_block_time,
    a.evt_tx_hash,
    a."_setToken" as token_address,
    b.symbol as symbol,
    case
    when c.evt_tx_hash is not null then 'Exchange'
    else 'Standard'
    end as contract_use_type,
    'Redeem' as evt_type,
    a."_redeemer" as wallet_address,
    a."_quantity"/1e18 as amount
from        setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed" a
inner join  dune_user_generated.indexcoop_tokens b on a."_setToken" = b.token_address
            and b.issuance_model = 'Standard'
left join   setprotocol_v2."ExchangeIssuance_evt_ExchangeRedeem" c on a.evt_tx_hash = c.evt_tx_hash 

union all

select 
    a.evt_block_time,
    a.evt_tx_hash,
    a."_setToken" as token_address,
    b.symbol as symbol,
    case
    when c.evt_tx_hash is not null then 'Exchange'
    else 'Standard'
    end as contract_use_type,
    'Issue' as evt_type,
    a."_issuer" as wallet_address,
    a."_quantity"/1e18 as amount
from        setprotocol_v2."DebtIssuanceModule_evt_SetTokenIssued" a
inner join  dune_user_generated.indexcoop_tokens b on a."_setToken" = b.token_address
            and b.issuance_model = 'Debt'
left join   setprotocol_v2."ExchangeIssuance_evt_ExchangeIssue" c on a.evt_tx_hash = c.evt_tx_hash 

union all

select 
    a.evt_block_time,
    a.evt_tx_hash,
    a."_setToken" as token_address,
    b.symbol as symbol,
    case
    when c.evt_tx_hash is not null then 'Exchange'
    else 'Standard'
    end as contract_use_type,
    'Redeem' as evt_type,
    a."_redeemer" as wallet_address,
    a."_quantity"/1e18 as amount
from        setprotocol_v2."DebtIssuanceModule_evt_SetTokenRedeemed" a
inner join  dune_user_generated.indexcoop_tokens b on a."_setToken" = b.token_address
            and b.issuance_model = 'Debt'
left join   setprotocol_v2."ExchangeIssuance_evt_ExchangeRedeem" c on a.evt_tx_hash = c.evt_tx_hash 

union all

select 
    a.evt_block_time,
    a.evt_tx_hash,
    a."_setToken" as token_address,
    b.symbol as symbol,
    case
    when c.evt_tx_hash is not null then 'Exchange'
    else 'Standard'
    end as contract_use_type,
    'Issue' as evt_type,
    a."_issuer" as wallet_address,
    a."_quantity"/1e18 as amount
from        setprotocol_v2."DebtIssuanceModuleV2_evt_SetTokenIssued" a
inner join  dune_user_generated.indexcoop_tokens b on a."_setToken" = b.token_address
            and b.issuance_model = 'Debt'
left join   setprotocol_v2."ExchangeIssuance_evt_ExchangeIssue" c on a.evt_tx_hash = c.evt_tx_hash 

union all

select 
    a.evt_block_time,
    a.evt_tx_hash,
    a."_setToken" as token_address,
    b.symbol as symbol,
    case
    when c.evt_tx_hash is not null then 'Exchange'
    else 'Standard'
    end as contract_use_type,
    'Redeem' as evt_type,
    a."_redeemer" as wallet_address,
    a."_quantity"/1e18 as amount
from        setprotocol_v2."DebtIssuanceModuleV2_evt_SetTokenRedeemed" a
inner join  dune_user_generated.indexcoop_tokens b on a."_setToken" = b.token_address
            and b.issuance_model = 'Debt'
left join   setprotocol_v2."ExchangeIssuance_evt_ExchangeRedeem" c on a.evt_tx_hash = c.evt_tx_hash 
