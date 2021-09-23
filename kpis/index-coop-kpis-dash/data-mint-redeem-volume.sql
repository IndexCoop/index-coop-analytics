-- https://dune.xyz/queries/163032

WITH mint_volume AS (

SELECT
    date_trunc('day', evt_block_time) AS day,
    SUM("_quantity"/1e18) as quantity
FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
WHERE "_setToken" IN ('\x33d63ba1e57e54779f7ddaeaa7109349344cf5f1')
GROUP BY 1

),

redeem_volume AS (

SELECT
    date_trunc('day', evt_block_time) AS day,
    -SUM("_quantity"/1e18) as quantity
FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed"
WHERE "_setToken" IN ('\x33d63ba1e57e54779f7ddaeaa7109349344cf5f1')
GROUP BY 1

),

data_price_feed AS (

    WITH prices_usd AS (
    
        SELECT
            date_trunc('day', minute) AS dt,
            AVG(price) AS price
        FROM prices.usd
        WHERE contract_address = '\x33d63ba1e57e54779f7ddaeaa7109349344cf5f1'
        GROUP BY 1
        ORDER BY 1
        
    ),
    
    data_swap AS (

    --eth/data sushi 0x208226200b45b82212b814f49efa643980a7bdd1
        
        SELECT
            date_trunc('hour', sw."evt_block_time") AS hour,
            ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
            ("amount1In" + "amount1Out")/1e18 AS a1_amt
        FROM sushi."Pair_evt_Swap" sw
        WHERE contract_address = '\x208226200b45b82212b814f49efa643980a7bdd1' -- liq pair address I am searching the price for
            AND sw.evt_block_time >= '2021-09-21'
    
    ),
    
    data_a1_prcs AS (
    
        SELECT 
            avg(price) a1_prc, 
            date_trunc('hour', minute) AS hour
        FROM prices.usd
        WHERE minute >= '2021-09-21'
            AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --weth as base asset
        GROUP BY 2
                    
    ),
    
    data_hours AS (
        
        SELECT generate_series('2021-09-21 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
        
    ),
    
    data_temp AS (
    
        SELECT
            h.hour,
            COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price, 
            COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as eth_price
        FROM data_hours h
        LEFT JOIN data_swap s ON s."hour" = h.hour 
        LEFT JOIN data_a1_prcs a ON h."hour" = a."hour"
        GROUP BY 1
    
    ),
    
    data_feed AS (
    
        SELECT
            hour,
            'DATA' AS product,
            (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price
        FROM data_temp
    
    ),
    
    data_price_feed AS (
    
        SELECT
            date_trunc('day', hour) AS dt,
            AVG(usd_price) AS price
        FROM data_feed
        -- WHERE date_trunc('day', hour) NOT IN (SELECT dt FROM prices_usd)
        WHERE usd_price IS NOT NULL
        GROUP BY 1
    
    ),
    
    data_price AS (
    
    SELECT
        *
    FROM prices_usd
    
    UNION ALL
    
    SELECT
        *
    FROM data_price_feed
    
    )
    
    SELECT
        *
    FROM data_price
    WHERE dt > '2021-09-20'
    ORDER BY 1

),

data_days AS (
    
    SELECT generate_series('2021-09-21'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
)

SELECT
    d.day,
    COALESCE(m.quantity, 0) AS mint_volume,
    COALESCE(r.quantity, 0) AS redeem_volume,
    COALESCE(m.quantity, 0) + COALESCE(r.quantity, 0) AS net_volume,
    COALESCE(m.quantity, 0) * p.price AS mint_in_dollars,
    COALESCE(r.quantity, 0) * p.price AS redeem_in_dollars,
    (COALESCE(m.quantity, 0) + COALESCE(r.quantity, 0)) * p.price AS net_volume_in_dollars,
    AVG(COALESCE(m.quantity, 0) + COALESCE(r.quantity, 0)) OVER (ORDER BY m.day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS av,
    AVG((COALESCE(m.quantity, 0) + COALESCE(r.quantity, 0)) * p.price) OVER (ORDER BY m.day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS av_in_dollars
FROM data_days d
LEFT JOIN mint_volume m ON d.day = m.day
LEFT JOIN redeem_volume r ON d.day = r.day
LEFT JOIN data_price_feed p ON d.day = p.dt
