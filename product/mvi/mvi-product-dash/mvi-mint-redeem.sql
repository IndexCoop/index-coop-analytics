-- https://duneanalytics.com/queries/42181

WITH mint_volume AS (

SELECT
    date_trunc('day', evt_block_time) AS day,
    SUM("_quantity"/1e18) as quantity
FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
WHERE "_setToken" IN ('\x72e364f2abdc788b7e918bc238b21f109cd634d7')
GROUP BY 1

),

redeem_volume AS (

SELECT
    date_trunc('day', evt_block_time) AS day,
    -SUM("_quantity"/1e18) as quantity
FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed"
WHERE "_setToken" IN ('\x72e364f2abdc788b7e918bc238b21f109cd634d7')
GROUP BY 1

),

mvi_daily_price_feed AS (

    WITH prices_usd AS (
    
        SELECT
            date_trunc('day', minute) AS dt,
            AVG(price) AS price
        FROM prices.usd
        WHERE symbol = 'MVI'
        GROUP BY 1
        ORDER BY 1
        
    ),
        
    mvi_swap AS (
    
    --eth/mvi uni        x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff
        
        SELECT
            date_trunc('hour', sw."evt_block_time") AS hour,
            ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
            ("amount1In" + "amount1Out")/1e18 AS a1_amt
        FROM uniswap_v2."Pair_evt_Swap" sw
        WHERE contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff' -- liq pair address I am searching the price for
            AND sw.evt_block_time >= '2021-04-06'
    
    ),
    
    mvi_a1_prcs AS (
    
        SELECT 
            avg(price) a1_prc, 
            date_trunc('hour', minute) AS hour
        FROM prices.usd
        WHERE minute >= '2021-04-07'
            AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --weth as base asset
        GROUP BY 2
                    
    ),
    
    mvi_hours AS (
        
        SELECT generate_series('2021-04-06 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
        
    ),
    
    mvi_temp AS (
    
    SELECT
        h.hour,
        COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price, 
        COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as eth_price
        -- a1_prcs."minute" AS minute
    FROM mvi_hours h
    LEFT JOIN mvi_swap s ON s."hour" = h.hour 
    LEFT JOIN mvi_a1_prcs a ON h."hour" = a."hour"
    GROUP BY 1
    
    ),
    
    mvi_feed AS (
    
    SELECT
        hour,
        'MVI' AS product,
        (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
        (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
    FROM mvi_temp
    
    ),
    
    mvi_price_feed AS (
    
        SELECT
            date_trunc('day', hour) AS dt,
            AVG(usd_price) AS price
        FROM mvi_feed
        WHERE date_trunc('day', hour) NOT IN (SELECT dt FROM prices_usd)
            AND usd_price IS NOT NULL
        GROUP BY 1
    
    ),
    
    mvi_price AS (
    
        SELECT
            *
        FROM prices_usd
        
        UNION ALL
        
        SELECT
            *
        FROM mvi_price_feed
    
    )
    
    SELECT
        *
    FROM mvi_price
    WHERE dt > '2021-04-01'
    ORDER BY 1


)

SELECT
    m.day,
    m.quantity AS mint_volume,
    r.quantity AS redeem_volume,
    m.quantity + r.quantity AS net_volume,
    m.quantity * p.price AS mint_in_dollars,
    r.quantity * p.price AS redeem_in_dollars,
    (m.quantity + r.quantity) * p.price AS net_volume_in_dollars,
    AVG(m.quantity + r.quantity) OVER (ORDER BY m.day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS av,
    AVG((m.quantity + r.quantity) * p.price) OVER (ORDER BY m.day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS av_in_dollars
FROM mint_volume m
JOIN redeem_volume r ON m.day = r.day
JOIN mvi_daily_price_feed p ON m.day = p.dt