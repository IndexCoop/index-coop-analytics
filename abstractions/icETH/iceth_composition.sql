-- https://dune.com/queries/1581818
create or replace view dune_user_generated.iceth_composition as

(
with

issue_events as (
select
    i."evt_block_time",
    i."evt_tx_hash",
    sum(e."value"/1e18) filter (where e."contract_address" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84' and e."from" = '\x0000000000000000000000000000000000000000') as iceth_amount,
    sum(e."value"/1e18) filter (where e."contract_address" = '\x1982b2F5814301d4e9a8b0201555376e62F82428' and e."to" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84') as aSTETH_amount,
    sum(e."value"/1e18) filter (where e."contract_address" = '\xF63B34710400CAd3e044cFfDcAb00a0f32E33eCf' and e."to" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84') as variableDebtWETH_amount
from        setprotocol_v2."DebtIssuanceModuleV2_evt_SetTokenIssued" i
left join   erc20."ERC20_evt_Transfer" e on i."evt_tx_hash" = e."evt_tx_hash"
where       "_setToken" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84'
group by    1,2
),

redeem_events as (
select
    i."evt_block_time",
    i."evt_tx_hash",
    sum(e."value"/1e18) filter (where e."contract_address" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84' and e."to" = '\x0000000000000000000000000000000000000000') as iceth_amount,
    sum(e."value"/1e18) filter (where e."contract_address" = '\x1982b2F5814301d4e9a8b0201555376e62F82428' and e."from" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84') as aSTETH_amount,
    sum(e."value"/1e18) filter (where e."contract_address" = '\xF63B34710400CAd3e044cFfDcAb00a0f32E33eCf' and e."from" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84') as variableDebtWETH_amount
from        setprotocol_v2."DebtIssuanceModuleV2_evt_SetTokenRedeemed" i
left join   erc20."ERC20_evt_Transfer" e on i."evt_tx_hash" = e."evt_tx_hash"
where       "_setToken" = '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84'
group by    1,2
),

all_events as (
select 
    evt_block_time, 
    evt_tx_hash, 	
    asteth_amount / iceth_amount as asteth,
    variabledebtweth_amount / iceth_amount as variabledebtweth
from issue_events
union
select 
    evt_block_time, 
    evt_tx_hash, 	
    asteth_amount / iceth_amount as asteth,
    variabledebtweth_amount / iceth_amount as variabledebtweth
from redeem_events
),

iceth_leverage_ratio as (
select
    hour,
    a.asteth,
    a.variabledebtweth,
    a.asteth / (a.asteth - a.variabledebtweth) as lev_ratio,
    a.asteth - a.variabledebtweth as parity_value
from    (
        select
            h.hour,
            (select evt_tx_hash from all_events where evt_block_time <= h.hour order by evt_block_time desc limit 1) as hash
        from        (select generate_series('2022-04-01 16:00', date_trunc('hour', now()), '1 hour') as hour) h
        ) t
inner join  all_events a on a.evt_tx_hash = t.hash
)

select * from iceth_leverage_ratio order by hour 
)
