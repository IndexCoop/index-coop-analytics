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
    
    data_sync AS (

    --eth/data sushi 0x208226200b45b82212b814f49efa643980a7bdd1
    
        SELECT
            date_trunc('hour', evt_block_time) AS hour,
            reserve1 / reserve0 AS eth_price,
            (reserve1 / reserve0) * p.price AS usd_price
        FROM sushi."Pair_evt_Sync" s
        LEFT JOIN prices.usd p ON p.symbol = 'WETH' AND p.minute = date_trunc('minute', s.evt_block_time)
        WHERE s.contract_address = '\x208226200b45b82212b814f49efa643980a7bdd1'
        ORDER BY 1

    ),
    
    data_hours AS (
        
        SELECT generate_series('2021-09-21 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
        
    ),
    
    data_temp AS (
    
        SELECT
            h.hour,
            COALESCE(AVG(usd_price), NULL) AS usd_price, 
            COALESCE(AVG(eth_price), NULL) as eth_price
        FROM data_hours h
        LEFT JOIN data_sync s ON s."hour" = h.hour 
        GROUP BY 1
    
    ),
    
    data_feed AS (
    
        SELECT
            hour,
            'DATA' AS product,
            (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
            (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
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