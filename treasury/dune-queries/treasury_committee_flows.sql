/*
    query here: https://duneanalytics.com/queries/47058

    --- INDEX Treasury ---

    Wallet / Address
    ('\x9467cfadc9de245010df95ec6a585a506a8ad5fc', -- Treasury Wallet
    '\xe2250424378b6a6dC912f5714cfd308a8D593986', -- Treasury Committee Wallet
    '\x26e316f5b3819264DF013Ccf47989Fb8C891b088' -- Community Treasury Year 1 Vesting
    )    
    

*/

-- Start Generalized Price Feed block - see generalized_price_feed.sql
WITH prices_usd AS (

    SELECT
        date_trunc('day', minute) AS dt
        , symbol
        , AVG(price) AS price
    FROM prices.usd
    WHERE symbol in ('INDEX', 'DPI', 'MVI', 'ETH2x-FLI', 'BTC2x-FLI')
    GROUP BY 1,2
)
    
, swaps AS (
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

    union all
    
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
, swap_a1_prcs AS (
    SELECT 
        avg(price) a1_prc
        , date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2020-09-10'
        AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --weth as base asset
    GROUP BY 2               
)
, uni_hours AS (
    SELECT generate_series('2020-09-10 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
)
, uni_temp AS (
    SELECT
        h.hour
        , s.symbol
        , COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price
        , COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as eth_price
        -- a1_prcs."minute" AS minute
    FROM uni_hours h
    LEFT JOIN swaps s ON h."hour" = s.hour 
    LEFT JOIN swap_a1_prcs a ON h."hour" = a."hour"
    GROUP BY 1,2
) 
, swap_feed AS (
    SELECT
        hour
        , symbol
        , (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (PARTITION BY symbol ORDER BY hour), NULL))[COUNT(usd_price) OVER (PARTITION BY symbol ORDER BY hour)] AS usd_price
        , (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (PARTITION BY symbol ORDER BY hour), NULL))[COUNT(eth_price) OVER (PARTITION BY symbol ORDER BY hour)] AS eth_price
    FROM uni_temp
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

)
, prices AS (
    SELECT *
    FROM prices_usd
    where dt > '2020-10-06'
    UNION ALL
    SELECT *
    FROM swap_price_feed
    where dt > '2020-10-06'
)
-- End price feed block - output is CTE "prices"
, wallets AS (
    SELECT 'INDEX' AS org
        , '\xe2250424378b6a6dC912f5714cfd308a8D593986'::bytea AS address
        , 'Treasury Committee' AS wallet
    /*
    union
    select 'INDEX' AS org
    , '\x26e316f5b3819264DF013Ccf47989Fb8C891b088'::bytea AS address
    , 'Community Treasury Year 1 Vesting' AS wallet
    */
)

, creation_days AS (
    SELECT
        date_trunc('day', block_time) AS day
    FROM ethereum.traces
    WHERE address IN (SELECT address FROM wallets)
    AND TYPE = 'create'
)
, weeks AS (
    SELECT 
        generate_series(date_trunc('week', MIN(day))
                        , date_trunc('week', NOW())
                        , '1 week') AS week -- Generate all weeks since the first contract
    FROM creation_days
)
, transfers AS (
    --ERC20 Tokens
    SELECT
        date_trunc('day', evt_block_time) AS day
        , "from" AS address
        , contract_address
        , sum(-value) AS outflow
        , 0 as inflow
    FROM erc20."ERC20_evt_Transfer"
    WHERE "from" IN (SELECT address FROM wallets)
    AND evt_block_time >= (SELECT min(day) FROM creation_days)
    GROUP BY 1,2,3
    
    UNION ALL

    SELECT
        date_trunc('day', evt_block_time) AS day
        , "to" AS address
        , contract_address
        , 0 as outflow
        , sum(value) AS inflow
    FROM erc20."ERC20_evt_Transfer"
    WHERE "to" IN (SELECT address FROM wallets)
    AND evt_block_time >= (SELECT min(day) FROM creation_days)
    GROUP BY 1,2,3
)
, transfers_week AS (
    SELECT
        date_trunc('week', tr.day) as week
        , tr.address
        , tok.symbol
        , sum(tr.inflow/10^(tok.decimals) * coalesce(p.price,0)) AS inflow_usd
        , sum(tr.outflow/10^(tok.decimals)* coalesce(p.price,0)) AS outflow_usd
    FROM transfers tr
    inner join erc20.tokens tok on tr.contract_address = tok.contract_address
    left join prices p on tok.symbol = p.symbol and p.dt = tr.day
    GROUP BY 1,2,3
)
SELECT
    w.week
--        t.address,
    -- , t.symbol
    , sum(coalesce(t.inflow_usd,0)) as inflow_usd
    , sum(coalesce(t.outflow_usd, 0)) as outflow_usd
    , sum(sum(coalesce(t.inflow_usd,0)) - sum(coalesce(t.outflow_usd, 0))) over 
        (order by w.week asc rows between unbounded preceding and current row) as balance
FROM weeks w
left join transfers_week t ON w.week = t.week
group by 1

