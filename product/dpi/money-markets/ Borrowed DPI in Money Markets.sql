-- https://dune.xyz/queries/152539
-- aDPI: \x6f634c6135d2ebd550000ac92f494f9cb8183dae (Aave V2)
-- crDPI: \x2a537fa9ffaea8c1a41d3c2b68a9cb791529366d (Cream V1)
-- cyDPI: \x7736ffb07104c0c400bb0cc9a7c228452a732992 (Cream V2 - IronBank)
-- fDPI-19: \xf06f65a6b7d2c401fcb8b3273d036d21fe2a5963 (Rari Fuse Pool 19 - IndexCoop)

WITH money_markets AS (
        
    SELECT 
        * 
    FROM (
    VALUES 
    ('\x6f634c6135d2ebd550000ac92f494f9cb8183dae', 'Aave V2'), 
    ('\x2a537fa9ffaea8c1a41d3c2b68a9cb791529366d', 'Cream V1'), 
    ('\x7736ffb07104c0c400bb0cc9a7c228452a732992', 'Cream V2 - IronBank'),
    ('\xf06f65a6b7d2c401fcb8b3273d036d21fe2a5963', 'Rari Fuse Pool 19')
    ) AS t (market_address, market)

),

mint_burn AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed" 
    WHERE "_setToken" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    GROUP BY 1
),

days AS (
    
    SELECT generate_series('2020-09-10'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM days d
    LEFT JOIN mint_burn m ON d.day = m.day
    
),

unit_supply AS (

    SELECT 
        DISTINCT
        day, 
        SUM(amount) OVER (ORDER BY day) AS dpi
    FROM units
    ORDER BY 1
    
)

, mm as (
SELECT
    a.*,
    b.market,
    c.dpi,
    a.amount / c.dpi AS percent_of_supply,
    a.amount_usd / a.amount as dpi_usd
FROM erc20."view_token_balances_daily" a
LEFT JOIN money_markets b ON a.wallet_address = b.market_address::bytea
LEFT JOIN unit_supply c ON a.day = c.day
WHERE a.wallet_address IN (SELECT market_address::bytea FROM money_markets)
AND a.token_address = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'::bytea
)

-- AAVE borrow
, aave_pay_borrow_temp as (
select date_trunc('day',"block_time") as day, sum("token_amount") as amount
from lending."borrow"
where"asset_address" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
and "project" = 'Aave' and "version" = '2'
group by 1

union all 

select date_trunc('day',"block_time") as day, sum(-"token_amount") as amount
from lending."repay"
where"asset_address" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
and "project" = 'Aave' and "version" = '2'
group by 1
)

, aave_pay_borrow as (
select day, sum(amount) as amount from aave_pay_borrow_temp
group by 1
)

, aave_total_borrow_w_gap_days as (
select day, amount,
sum(amount) over (order by day asc) as totalborrow,
lead(day, 1, now()) over (order by day) as next_day 
from aave_pay_borrow
)

, aave_total_borrow_daily as (
select d.day, at.amount, at.totalborrow as borrowed, (at.amount+at.totalborrow) as total_tvl,  'Aave V2' as market
from aave_total_borrow_w_gap_days at
inner join days d on at.day <= d.day and d.day < at.next_day
)

-- Cream V1 borrow
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

, creamv1_all_days as ( --borrow rate = interest earned/borrowed
select d.day, cw.cash, cw.borrowed, cw.total_tvl, cw.market, cw.next_day 
from creamv1_w_gap_days cw
inner join days d on cw.day <= d.day and d.day < cw.next_day
)

--Cream V2 - IronBank
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

, creamv2_all_days as (  
select d.day, cr.cash, cr.borrowed, cr.total_tvl, cr.market, cr.next_day --borrow rate = interest earned/borrowed
from creamv2_w_gap_days cr
inner join days d on cr.day <= d.day and d.day < cr.next_day
)

-- 'Rari Fuse Pool 19'
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

, rari_all_days as (  
select d.day, r.cash, r.borrowed, r.total_tvl, r.market, r.next_day --borrow rate = interest earned/borrowed
from rari_w_gap_days r
inner join days d on r.day <= d.day and d.day < r.next_day
)



select m.*
, case
    when m.market = 'Cream V1' then ct.borrowed
    when m.market = 'Cream V2 - IronBank' then ct2.borrowed
    when m.market = 'Aave V2' then at.borrowed
    when m.market = 'Rari Fuse Pool 19' then r.borrowed
    end as borrowed
, case
    when m.market = 'Cream V1' then (ct.borrowed)/(ct.total_tvl)
    when m.market = 'Cream V2 - IronBank' then (ct2.borrowed)/(ct2.total_tvl)
    when m.market = 'Aave V2' then (at.borrowed)/(m.amount+at.borrowed)
    when m.market = 'Rari Fuse Pool 19' then (r.borrowed)/(r.total_tvl)
    end as utilization
, case
    when m.market = 'Cream V1' then ct.borrowed * m.dpi_usd
    when m.market = 'Cream V2 - IronBank' then ct2.borrowed * m.dpi_usd
    when m.market = 'Aave V2' then at.borrowed * m.dpi_usd
    when m.market = 'Rari Fuse Pool 19' then r.borrowed * m.dpi_usd
    end as tvl_usd

from mm m
left join creamv1_all_days ct
on m.day = ct.day and m.market = ct.market
left join creamv2_all_days ct2
on m.day = ct2.day and m.market = ct2.market
left join aave_total_borrow_daily at
on m.day = at.day and m.market = at.market
left join rari_all_days r
on m.day = r.day and m.market = r.market


