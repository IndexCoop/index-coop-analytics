-- query link: https://duneanalytics.com/queries/71179
-- Query to estimate how much of the Index USD price is represented by assets on hand in the treasury
-- The assets included:
--      * DPI
--      * USDC

-- For "Circulating Supply", we exclude the INDEX that's locked 
-- in the Community, Set, and Defi Pulse vesting schedules

-- Start Generalized Price Feed block - see generalized_price_feed.sql

WITH prices_usd AS (

    SELECT
        date_trunc('day', minute) AS dt
        , symbol
        , decimals
        , AVG(price) AS price
    FROM prices.usd
    WHERE symbol in ('INDEX', 'DPI', 'MVI', 'ETH2x-FLI', 'BTC2x-FLI', 'USDC')
    GROUP BY 1,2,3
)
    
, eth_swaps AS (
    -- Uniswap price feed
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour
        , case 
            when contract_address = '\x3452A7f30A712e415a0674C0341d44eE9D9786F9' then 'INDEX'
            when contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991' then 'DPI'
            when contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff' then 'MVI'
            when contract_address = '\xf91c12dae1313d0be5d7a27aa559b1171cc1eac5' then 'ETH2x-FLI'
        end as symbol
        , ("amount0In" + "amount0Out")/1e18 AS a0_amt
        , ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM uniswap_v2."Pair_evt_Swap" sw
    WHERE contract_address in ( '\x3452A7f30A712e415a0674C0341d44eE9D9786F9' -- liq pair addresses I am searching the price for
                                , '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
                                , '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff' 
                                , '\xf91c12dae1313d0be5d7a27aa559b1171cc1eac5' )
        AND sw.evt_block_time >= '2020-09-10'
)
, btc_swaps as (   
    -- Sushi price feed
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour
        , 'BTC2x-FLI' as symbol
        , ("amount0In" + "amount0Out")/1e18 AS a0_amt
        , ("amount1In" + "amount1Out")/1e8 AS a1_amt
    FROM sushi."Pair_evt_Swap" sw
    WHERE contract_address = '\x164fe0239d703379bddde3c80e4d4800a1cd452b' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2021-05-11'

)

, swap_a1_eth_prcs AS (

    SELECT 
        avg(price) a1_prc
        , date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2020-09-10'
        AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --weth as base asset
    GROUP BY 2                
)

, swap_a1_btc_prcs as (

    SELECT 
        avg(price) a1_prc, 
        date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2021-05-11'
        AND contract_address ='\x2260fac5e5542a773aa44fbcfedf7c193bc2c599' --wbtc as base asset
    GROUP BY 2
)

, swap_hours AS (
    
    SELECT generate_series('2020-09-10 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
    
)
, eth_temp AS (

    SELECT
        h.hour
        , s.symbol
        , COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price
        -- , COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as asset_price
        -- a1_prcs."minute" AS minute
    FROM swap_hours h
    LEFT JOIN eth_swaps s ON h."hour" = s.hour 
    LEFT JOIN swap_a1_eth_prcs a ON h."hour" = a."hour"
    GROUP BY 1,2

) 
, btc_temp as (
    SELECT
        h.hour
        , s.symbol
        , COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price
        -- , COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as asset_price
        -- a1_prcs."minute" AS minute
    FROM swap_hours h
    LEFT JOIN btc_swaps s ON h."hour" = s.hour 
    LEFT JOIN swap_a1_btc_prcs a ON h."hour" = a."hour"
    GROUP BY 1,2
)
, swap_temp as (
    select * from eth_temp
    union
    select * from btc_temp
)
, swap_feed AS (
    SELECT
        hour
        , symbol
        , (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (PARTITION BY symbol ORDER BY hour), NULL))[COUNT(usd_price) OVER (PARTITION BY symbol ORDER BY hour)] AS usd_price
        -- , (ARRAY_REMOVE(ARRAY_AGG(asset_price) OVER (PARTITION BY symbol ORDER BY hour), NULL))[COUNT(asset_price) OVER (PARTITION BY symbol ORDER BY hour)] AS asset_price
    FROM swap_temp
)
, swap_price_feed AS ( -- only include the uni feed when there's no corresponding price in prices_usd

    SELECT
        date_trunc('day', hour) AS dt
        , u.symbol
        , AVG(usd_price) AS price
    FROM swap_feed u
    left join prices_usd p on date_trunc('day', u.hour) = p.dt
        and u.symbol = p.symbol
    WHERE p.dt is null
        AND usd_price IS NOT NULL
    GROUP BY 1, 2

),

prices AS (

SELECT
    *
FROM prices_usd

UNION ALL

SELECT dt  
    , symbol
    , 18 as decimals -- all the INDEX tokens have 18 decimals
    , price
FROM swap_price_feed

)
-- End price feed block - output is CTE "prices"
, wallets AS (
    select '\x9467cfadc9de245010df95ec6a585a506a8ad5fc'::bytea AS address
    , 'Treasury Wallet' AS address_alias
    union all
        select '\x26e316f5b3819264DF013Ccf47989Fb8C891b088'::bytea as address
        , 'Community Treasury Year 1 Vesting' as address_alias
    union all
    select '\xd89C642e52bD9c72bCC0778bCf4dE307cc48e75A'::bytea as address
        , 'Community Treasury Year 2 Vesting' as address_alias
    union all
    select '\x71F2b246F270c6AF49e2e514cA9F362B491Fbbe1'::bytea as address
        , 'Community Treasury Year 3 Vesting' as address_alias
    union all
    select '\xf64d061106054Fe63B0Aca68916266182E77e9bc'::bytea as address
        , 'Set Labs Year 1 Vesting' as address_alias
    union all
    select '\x4c11dfd35a4fe079b41d5d9729ed34c00d487712'::bytea as address
        , 'Set Labs Year 2 Vesting' as address_alias
    union all
    select '\x0D627ca04A97219F182DaB0Dc2a23FB4a5B02A9D'::bytea as address
        , 'Set Labs Year 3 Vesting' as address_alias
    union all
    select '\x5c29aa6761803bcfda7f683eaa0ff9bddda3649d'::bytea as address
        , 'Defi Pulse Year 1 Vesting' as address_alias
    union all
    select '\xce3c6312385fcf233ab0de574b0cb1a588566c3f'::bytea as address
        , 'Defi Pulse Year 2 Vesting' as address_alias
    union all
    select '\x0f58793e8cf39d6b60919ffaf773a7f95a568146'::bytea as address
        , 'Defi Pulse Year 3 Vesting' as address_alias
)

, creation_days AS (
    SELECT
        date_trunc('day', block_time) AS day
    FROM ethereum.traces
    WHERE address IN (SELECT address FROM wallets)
    AND TYPE = 'create'
)
, days AS (
    SELECT 
        generate_series(MIN(day), date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    FROM creation_days
)
, transfers AS (
    --ERC20 Tokens
    SELECT
        date_trunc('day', evt_block_time) AS day,
        "from" AS address,
        contract_address,
        sum(-value) AS amount
    FROM erc20."ERC20_evt_Transfer"
    WHERE "from" IN (SELECT address FROM wallets)
    AND evt_block_time >= (SELECT min(day) FROM creation_days)
    GROUP BY 1,2,3
    
    UNION ALL

    SELECT
        date_trunc('day', evt_block_time) AS day,
        "to" AS address,
        contract_address,
        sum(value) AS amount
    FROM erc20."ERC20_evt_Transfer"
    WHERE "to" IN (SELECT address FROM wallets)
    AND evt_block_time >= (SELECT min(day) FROM creation_days)
    GROUP BY 1,2,3
)

, decimals as (
    select distinct contract_address
    , decimals
    from prices.usd
    WHERE symbol in ('INDEX', 'DPI', 'MVI', 'ETH2x-FLI', 'BTC2x-FLI', 'USDC')
)

, transfers_day AS (
    SELECT
        t.day,
        t.address,
        t.contract_address,
        sum(t.amount/10^coalesce(d.decimals,18)) AS change 
    FROM transfers t
    left join decimals d on t.contract_address = d.contract_address
    GROUP BY 1,2,3
)

, balances_w_gap_days AS (
    SELECT
        day,
        address,
        contract_address,
        sum(change) OVER (PARTITION BY address, contract_address ORDER BY day) AS "balance",
        lead(day, 1, now()) OVER (PARTITION BY address, contract_address ORDER BY day) AS next_day
    FROM transfers_day
)

, balances_all_days AS (
    SELECT
        d.day,
        b.address,
        b.contract_address,
        sum(b.balance) AS "balance"
    FROM balances_w_gap_days b
    INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
    GROUP BY 1,2,3
    ORDER BY 1,2,3
)
, usd_value_all_days as (
    SELECT
        b.day,
    --    b.address,
        w.address_alias,
    --    w.org,
        b.contract_address,
        p.symbol AS token,
        b.balance,
        p.price,
        b.balance * coalesce(p.price,0) AS usd_value
        , rank() over (order by b.day desc)
    FROM balances_all_days b
    left join erc20.tokens t on b.contract_address = t.contract_address
    inner JOIN prices p ON t.symbol = p.symbol AND b.day = p.dt
    LEFT OUTER JOIN wallets w ON b.address = w.address
    where b.day <= '{{end_date}}'
)
, current_balances as (
select 
    address_alias
    , token
    , balance
    , usd_value
    , contract_address
from usd_value_all_days
where rank = 1
)
, locked_index_supply as (
    select sum(balance) as locked_index_supply
    from current_balances
    where address_alias in 
        -- being explicit about which accounts count as "locked"
        ('Community Treasury Year 1 Vesting'
        , 'Community Treasury Year 2 Vesting'
        , 'Community Treasury Year 3 Vesting'
        , 'Set Labs Year 1 Vesting' 
        , 'Set Labs Year 2 Vesting' 
        , 'Set Labs Year 3 Vesting' 
        , 'Defi Pulse Year 1 Vesting'
        , 'Defi Pulse Year 2 Vesting'
        , 'Defi Pulse Year 3 Vesting'
        )
)
, treasury_wallet_value as (
    select sum(case when token = 'DPI' then usd_value else 0 end) as dpi_usd_value
        , sum(case when token = 'USDC' then usd_value else 0 end) as usdc_usd_value
    from current_balances
    where address_alias = 'Treasury Wallet'
)
, index_mint_events as (
                SELECT t.symbol, e.*,       
                CASE WHEN "from" = '\x0000000000000000000000000000000000000000' THEN (value*1.0/10^decimals) ELSE 0 END AS minted,
                CASE WHEN "to"   = '\x0000000000000000000000000000000000000000' THEN (value*1.0/10^decimals) ELSE 0 END AS burned
                FROM erc20."ERC20_evt_Transfer" e
                left join erc20.tokens t on t.contract_address = e.contract_address
                WHERE e.contract_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
                and not ("from" = '\x0000000000000000000000000000000000000000'
                        and "to"   = '\x0000000000000000000000000000000000000000')
                and ("from" = '\x0000000000000000000000000000000000000000'
                        or "to"   = '\x0000000000000000000000000000000000000000')
)
, index_total_supply as (
    select sum(minted) - sum(burned) as total_index_supply
    from index_mint_events
)
select dpi_usd_value
    , usdc_usd_value
    , total_index_supply
    , locked_index_supply
    , total_index_supply - locked_index_supply as circulating_supply
    , (dpi_usd_value + usdc_usd_value) / total_index_supply as usd_per_total_supply
    , (dpi_usd_value + usdc_usd_value) / (total_index_supply - locked_index_supply) as usd_per_circ_supply
from index_total_supply 
join locked_index_supply on 1=1 
join treasury_wallet_value on 1=1

