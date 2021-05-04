-- https://duneanalytics.com/queries/35663

WITH mint_volume AS (

SELECT
    date_trunc('day', evt_block_time) AS day,
    SUM("_quantity"/1e18) as quantity
FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
WHERE "_setToken" IN ('\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b')
GROUP BY 1

),

redeem_volume AS (

SELECT
    date_trunc('day', evt_block_time) AS day,
    -SUM("_quantity"/1e18) as quantity
FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed"
WHERE "_setToken" IN ('\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b')
GROUP BY 1

),

dpi_daily_price_feed AS (

    WITH prices_usd AS (
    
        SELECT
            date_trunc('day', minute) AS dt,
            AVG(price) AS price
        FROM prices.usd
        WHERE symbol = 'DPI'
        GROUP BY 1
        ORDER BY 1
        
    ),
        
    dpi_swap AS (
    
    --eth/dpi uni        x4d5ef58aac27d99935e5b6b4a6778ff292059991
        
        SELECT
            date_trunc('hour', sw."evt_block_time") AS hour,
            ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
            ("amount1In" + "amount1Out")/1e18 AS a1_amt
        FROM uniswap_v2."Pair_evt_Swap" sw
        WHERE contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991' -- liq pair address I am searching the price for
            AND sw.evt_block_time >= '2020-09-10'
    
    ),
    
    dpi_a1_prcs AS (
    
        SELECT 
            avg(price) a1_prc, 
            date_trunc('hour', minute) AS hour
        FROM prices.usd
        WHERE minute >= '2020-09-10'
            AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --weth as base asset
        GROUP BY 2
                    
    ),
    
    dpi_hours AS (
        
        SELECT generate_series('2020-09-10 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
        
    ),
    
    dpi_temp AS (
    
    SELECT
        h.hour,
        COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price, 
        COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as eth_price
        -- a1_prcs."minute" AS minute
    FROM dpi_hours h
    LEFT JOIN dpi_swap s ON s."hour" = h.hour 
    LEFT JOIN dpi_a1_prcs a ON h."hour" = a."hour"
    GROUP BY 1
    
    ),
    
    dpi_feed AS (
    
    SELECT
        hour,
        'DPI' AS product,
        (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
        (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
    FROM dpi_temp
    
    ),
    
    dpi_price_feed AS (
    
        SELECT
            date_trunc('day', hour) AS dt,
            AVG(usd_price) AS price
        FROM dpi_feed
        WHERE date_trunc('day', hour) NOT IN (SELECT dt FROM prices_usd)
            AND usd_price IS NOT NULL
        GROUP BY 1
    
    ),
    
    dpi_price AS (
    
    SELECT
        *
    FROM prices_usd
    
    UNION ALL
    
    SELECT
        *
    FROM dpi_price_feed
    
    )
    
    SELECT
        *
    FROM dpi_price
    WHERE dt > '2020-09-10'
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
JOIN dpi_daily_price_feed p ON m.day = p.dt