-- https://dune.com/queries/1248405/

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

minutes as (
select generate_series(date_trunc('minute', now() - interval '30 days'), date_trunc('minute', now()), '1 minute') as minute
),

swaps as (
select
    minute,
    first_value(price) over (partition by price_partition order by minute) as price
from    (
        select
            m.minute,
            t.price,
            sum(case when t.price is null then 0 else 1 end) over (order by m.minute) as price_partition
        from        (select generate_series(date_trunc('minute', now() - interval '30 days'), date_trunc('minute', now()), '1 minute') as minute) m
        left join   (
                    select
                        date_trunc('minute', "evt_block_time") as minute,
                        avg("sqrtPriceX96"^2 /(2^192)) as price
                    from    uniswap_v3."Pair_evt_Swap"
                    where   "contract_address" = '\xe5d028350093a743a9769e6fd7f5546eeddaa320'
                    and     "evt_block_time" >= now() - interval '30 days'
                    group by 1
                    ) t on m.minute = t.minute
        ) t1
),

composition as (
select
    t.minute,
    p1.price as steth_price,
    p2.price as weth_price,
    asteth,
    variabledebtweth,
    (p1.price * asteth - p2.price * variabledebtweth) / p2.price as nav,
    s.price as price
from        (
            select
                m.minute,
                (select evt_tx_hash from all_events where evt_block_time <= minute order by evt_block_time desc limit 1) as hash
            from minutes m
            ) t
left join   all_events a on t.hash = a.evt_tx_hash
left join   prices.usd p1 on t.minute = p1.minute and p1.contract_address = '\xae7ab96520de3a18e5e111b5eaab095312d7fe84' -- stETH
left join   prices.layer1_usd p2 on t.minute = p2.minute and p2.symbol = 'ETH'
left join   swaps s on t.minute = s.minute
)

select 
    *, 
    price/nav-1 as premium_discount, 
    (price/nav-1)*100 as premium_discount_text 
from composition 
where nav is not null 
and price is not null 
and minute >= now() - interval '1 day'
order by minute desc
