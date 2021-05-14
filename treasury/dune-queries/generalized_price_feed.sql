/*  dune query here: https://duneanalytics.com/queries/46532
    This query is to create a generalized price feed for INDEX products by merging
    swap prices in and inferring the USD price from the wETH or wBTC price when prices.usd is unavailable.
    It's designed to be used as part of the CTE pipeline for other queries.
    The tokens covered right now:
        - 'INDEX', uni_v2 contract address = '\x3452A7f30A712e415a0674C0341d44eE9D9786F9'
        - 'DPI', uni_v2 contract address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
        - 'MVI', uni_v2 contract address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff'
        - 'ETH2x-FLI', uni_v2 contract address = '\xf91c12dae1313d0be5d7a27aa559b1171cc1eac5'
        - 'BTC2x-FLI', sushi contract address = '\x164fe0239d703379bddde3c80e4d4800a1cd452b'
*/ 

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

),

index_price AS (

SELECT
    *
FROM prices_usd

UNION ALL

SELECT
    *
FROM swap_price_feed

)

SELECT
    *
FROM index_price
WHERE dt > '2020-10-06'
ORDER BY 1