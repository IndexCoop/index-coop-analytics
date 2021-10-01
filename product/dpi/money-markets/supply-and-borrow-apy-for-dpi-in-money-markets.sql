-- https://dune.xyz/queries/149195
-- aDPI: \x6f634c6135d2ebd550000ac92f494f9cb8183dae (Aave V2)
-- crDPI: \x2a537fa9ffaea8c1a41d3c2b68a9cb791529366d (Cream V1)
-- cyDPI: \x7736ffb07104c0c400bb0cc9a7c228452a732992 (Cream V2 - IronBank)
-- fDPI-19: \xf06f65a6b7d2c401fcb8b3273d036d21fe2a5963 (Rari Fuse Pool 19 - IndexCoop)

--AAVE Deposit APY percentDepositAPY = 100 * liquidityRate/RAY * https://docs.aave.com/developers/guides/apy-and-apr#fetch-data
with 

money_markets as (
        
select * 
from (
    values 
    ('\x6f634c6135d2ebd550000ac92f494f9cb8183dae', 'Aave V2'), 
    ('\x2a537fa9ffaea8c1a41d3c2b68a9cb791529366d', 'Cream V1'), 
    ('\x7736ffb07104c0c400bb0cc9a7c228452a732992', 'Cream V2 - IronBank'),
    ('\xf06f65a6b7d2c401fcb8b3273d036d21fe2a5963', 'Rari Fuse Pool 19')
    ) as t (market_address, market)
)


, days AS (
    
    SELECT generate_series('2020-11-06'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract

)
, mm_w_days as (
select * from money_markets
cross join days
)

, creamv1_w_gap_days as (
select 
avg("cashPrior"/1e18) as cash,
avg("interestAccumulated"/1e18) as interest_earned, 
avg("totalBorrows"/1e18) as borrowed, 
avg("cashPrior"/1e18) + avg("totalBorrows"/1e18) as total_tvl,
'Cream V1' as market,
date_trunc('day',evt_block_time) as day,
lead(date_trunc('day',evt_block_time), 1, now()) over (order by date_trunc('day',evt_block_time)) as next_day 
from creamfinance."CErc20Delegate_evt_AccrueInterest"
where contract_address = '\x2a537fa9ffaea8c1a41d3c2b68a9cb791529366d'
and ("totalBorrows"/1e18) > 0
group by 6
order by 6 desc
)

 --borrow rate = interest earned/borrowed
 --supply rate = interest earned/tvl
, creamv1_all_days as (
select d.day, cw.cash, cw.interest_earned/cw.total_tvl * 365  as supplyrate, (cw.interest_earned/cw.borrowed) * 365 as borrowrate, cw.total_tvl, cw.market, cw.next_day 
from creamv1_w_gap_days cw
inner join days d on cw.day <= d.day and d.day < cw.next_day
)

, creamv2_w_gap_days as (
select 
avg("cashPrior"/1e18) as cash,
avg("interestAccumulated"/1e18) as interest_earned, 
avg("totalBorrows"/1e18) as borrowed, 
avg("cashPrior"/1e18) + avg("totalBorrows"/1e18) as total_tvl,
'Cream V2 - IronBank' as market,
date_trunc('day',evt_block_time) as day,
lead(date_trunc('day',evt_block_time), 1, now()) over (order by date_trunc('day',evt_block_time)) as next_day 
from creamfinance_v2."CErc20Delegator_evt_AccrueInterest"
where contract_address = '\x7736ffb07104c0c400bb0cc9a7c228452a732992'
and ("totalBorrows"/1e18) > 0
group by 6
order by 6 desc
)

 --borrow rate = interest earned/borrowed
 --supply rate = interest earned/tvl
, creamv2_all_days as ( 
select d.day, cr.cash, (cr.interest_earned/cr.total_tvl) * 365 as supplyrate, (cr.interest_earned/cr.borrowed) * 365 as borrowrate, cr.total_tvl, cr.market, cr.next_day --borrow rate = interest earned/borrowed
from creamv2_w_gap_days cr
inner join days d on cr.day <= d.day and d.day < cr.next_day
)

, aave_supply_w_gap_days as ( 
select "output_liquidityRate"/1e27 as supplyrate,
'Aave V2' as market,
date_trunc('day',"call_block_time") as day,
lead(date_trunc('day',"call_block_time"), 1, now()) over (order by date_trunc('day',"call_block_time")) as next_day
from aave_v2."ProtocolDataProvider_call_getUserReserveData"
where "asset" =  '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
order by 3 desc
)

, rari_w_gap_days as (
select 
avg("cashPrior"/1e18) as cash,
avg("interestAccumulated"/1e18) as interest_earned, 
avg("totalBorrows"/1e18) as borrowed, 
avg("cashPrior"/1e18) + avg("totalBorrows"/1e18) as total_tvl,
'Rari Fuse Pool 19' as market,
date_trunc('day',evt_block_time) as day,
lead(date_trunc('day',evt_block_time), 1, now()) over (order by date_trunc('day',evt_block_time)) as next_day
from rari_capital."CErc20Delegate_evt_AccrueInterest"
where contract_address = '\xf06f65a6b7d2c401fcb8b3273d036d21fe2a5963'
group by 6
order by 6 desc
)

 --borrow rate = interest earned/borrowed
 --supply rate = interest earned/tvl
 
, rari_all_days as (
select d.day, r.cash, r.interest_earned/r.total_tvl * 365  as supplyrate, 
case 
    when  r.borrowed = 0 then 0
    else (r.interest_earned/r.borrowed) * 365 
end as borrowrate, 
r.total_tvl, r.market, r.next_day 
from rari_w_gap_days r
inner join days d on r.day <= d.day and d.day < r.next_day
)

, aave_borrow_w_gap_days as (
select sum("amount"/1e18) over (order by evt_block_time asc) as totalborrow,
"amount"/1e18 as amount,
"borrowRate"/1e27 as borrowrate,
'Aave V2' as market,
date_trunc('day',evt_block_time) as day,
lead(date_trunc('day',evt_block_time), 1, now()) over (order by date_trunc('day',evt_block_time)) as next_day
from aave_v2."LendingPool_evt_Borrow"
where "reserve" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
order by 5 desc
)

, aave_all_borrow_days as (
select d.day, a.borrowrate, a.market, a.next_day 
from aave_borrow_w_gap_days a
inner join days d on a.day <= d.day and d.day < a.next_day
)	

, aave_all_days as (
select aa.day, aa.borrowrate, a.supplyrate,
case 
    when aa.market = 'Aave V2' then 'Aave V2'
    when a.market = 'Aave V2 'then 'Aave V2'
end as market
from aave_all_borrow_days aa
inner join aave_supply_w_gap_days a on a.day <= aa.day and aa.day < a.next_day
)




select d.day, d.market,
case
    when cv.market = d.market then cv.supplyrate
    when ct.market = d.market then ct.supplyrate
    when at.market = d.market then at.supplyrate
    when r.market = d.market then r.supplyrate
    end as supplyrate,
case
    when cv.market = d.market then cv.borrowrate
    when ct.market = d.market then ct.borrowrate
    when at.market = d.market then at.borrowrate
    when r.market = d.market then r.borrowrate
    end as borrowrate
from mm_w_days d
left join creamv1_all_days cv
on d.day = cv.day and cv.market = d.market 
left join creamv2_all_days ct
on d.day = ct.day and ct.market = d.market
left join aave_all_days at
on d.day = at.day and at.market = d.market
left join rari_all_days r
on d.day = r.day and r.market = r.market
where cv.supplyrate is not null
or ct.supplyrate is not null
or at.supplyrate is not null
or r.supplyrate is not null
or cv.borrowrate is not null
or ct.borrowrate is not null
or at.borrowrate is not null
or r.borrowrate is not null