-- drop table if exists dune_user_generated.indexcoop_iceth_nav_minute  cascade
create table if not exists dune_user_generated.indexcoop_iceth_nav_minute as

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

-- There is no NAV before 2022-03-21 14:06
-- End cache as of 2022-10-31 23:59
minutes as (
select generate_series('2022-03-21 14:06'::timestamp, '2022-11-01 00:00'::timestamp, '1 minute') as minute
)

select
    minute,
    nav_steth,
    (steth_price * asteth - weth_price * variabledebtweth) / weth_price as nav_eth,
    (steth_price * asteth - weth_price * variabledebtweth) as nav_usd,
    steth_price, 
    weth_price,
    steth_price / weth_price as steth_eth_price,
    (asteth * steth_price) / (asteth * steth_price - variabledebtweth * weth_price)  as true_leverage_ratio,
    (asteth) / (asteth - variabledebtweth) as target_leverage_ratio
from    (
        select
            minute,
            first_value(steth_price) over (partition by steth_price_part order by minute) as steth_price,
            first_value(weth_price) over (partition by weth_price_part order by minute) as weth_price,
            asteth, variabledebtweth,
            asteth - variabledebtweth as nav_steth
        from    (
                select
                    t.minute,
                    p1.price as steth_price,
                    p2.price as weth_price,
                    sum(case when p1.price is null then 0 else 1 end) over (order by t.minute) as steth_price_part,
                    sum(case when p2.price is null then 0 else 1 end) over (order by t.minute) as weth_price_part,
                    asteth,
                    variabledebtweth
                from        (
                            select
                                m.minute,
                                (select evt_tx_hash from all_events where evt_block_time <= minute order by evt_block_time desc limit 1) as hash
                            from minutes m
                            ) t
                left join   all_events a on t.hash = a.evt_tx_hash
                left join   prices.usd p1 on t.minute = p1.minute and p1.contract_address = '\xae7ab96520de3a18e5e111b5eaab095312d7fe84' -- stETH
                left join   prices.layer1_usd p2 on t.minute = p2.minute and p2.symbol = 'ETH'
                ) t1
        ) t2
where minute < date_trunc('minute', '2022-11-01'::date)
)
