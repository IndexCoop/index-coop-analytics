-- https://duneanalytics.com/queries/34732

WITH prices_usd AS (

    SELECT
        date_trunc('day', minute) AS dt,
        AVG(price) AS price
    FROM prices.usd
    WHERE symbol = 'INDEX'
    GROUP BY 1
    ORDER BY 1
    
),
    
index_swap AS (

--eth/index uni        x3452A7f30A712e415a0674C0341d44eE9D9786F9
    
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour,
        ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
        ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM uniswap_v2."Pair_evt_Swap" sw
    WHERE contract_address = '\x3452A7f30A712e415a0674C0341d44eE9D9786F9' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2020-09-10'

),

index_a1_prcs AS (

    SELECT 
        avg(price) a1_prc, 
        date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2020-09-10'
        AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --weth as base asset
    GROUP BY 2
                
),

index_hours AS (
    
    SELECT generate_series('2020-09-10 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
    
),

index_temp AS (

SELECT
    h.hour,
    COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price, 
    COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as eth_price
    -- a1_prcs."minute" AS minute
FROM index_hours h
LEFT JOIN index_swap s ON s."hour" = h.hour 
LEFT JOIN index_a1_prcs a ON h."hour" = a."hour"
GROUP BY 1

),

index_feed AS (

SELECT
    hour,
    'INDEX' AS product,
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
    (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
FROM index_temp

),

index_price_feed AS (

    SELECT
        date_trunc('day', hour) AS dt,
        AVG(usd_price) AS price
    FROM index_feed
    WHERE date_trunc('day', hour) NOT IN (SELECT dt FROM prices_usd)
        AND usd_price IS NOT NULL
    GROUP BY 1

),

index_price AS (

SELECT
    *
FROM prices_usd

UNION ALL

SELECT
    *
FROM index_price_feed

)

SELECT
    *
FROM index_price
WHERE dt > '2020-10-06'
ORDER BY 1