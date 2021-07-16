-- https://duneanalytics.com/queries/81055/161674

WITH mint_volume AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS quantity
    FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenIssued"
    WHERE "_setToken" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    GROUP BY 1

),

redeem_volume AS (

SELECT
    date_trunc('day', evt_block_time) AS day,
    -SUM("_quantity"/1e18) as quantity
FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenRedeemed"
WHERE "_setToken" IN ('\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd')
GROUP BY 1

),

eth2x_daily_price_feed AS (

    WITH prices_usd AS (
    
        SELECT
            date_trunc('day', minute) AS dt,
            AVG(price) AS price
        FROM prices.usd
        WHERE symbol = 'ETH2x-FLI'
        GROUP BY 1
        ORDER BY 1
        
    ),
    
    fli_swap AS (
    
    --eth/fli uni        xf91c12dae1313d0be5d7a27aa559b1171cc1eac5
        
        SELECT
            date_trunc('hour', sw."evt_block_time") AS hour,
            ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
            ("amount1In" + "amount1Out")/1e18 AS a1_amt
        FROM uniswap_v2."Pair_evt_Swap" sw
        WHERE contract_address = '\xf91c12dae1313d0be5d7a27aa559b1171cc1eac5' -- liq pair address I am searching the price for
            AND sw.evt_block_time >= '2020-03-12'
    
    ),
    
    fli_a1_prcs AS (
    
        SELECT 
            avg(price) a1_prc, 
            date_trunc('hour', minute) AS hour
        FROM prices.usd
        WHERE minute >= '2021-03-12'
            AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --weth as base asset
        GROUP BY 2
                    
    ),
    
    fli_hours AS (
        
        SELECT generate_series('2021-03-12 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
        
    ),
    
    fli_temp AS (
    
    SELECT
        h.hour,
        COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price, 
        COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as eth_price
        -- a1_prcs."minute" AS minute
    FROM fli_hours h
    LEFT JOIN fli_swap s ON s."hour" = h.hour 
    LEFT JOIN fli_a1_prcs a ON h."hour" = a."hour"
    GROUP BY 1
    
    ),
    
    fli_feed AS (
    
    SELECT
        hour,
        'ETH2x-FLI' AS product,
        (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
        (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
    FROM fli_temp
    
    ),
    
    fli_price_feed AS (
    
        SELECT
            date_trunc('day', hour) AS dt,
            AVG(usd_price) AS price
        FROM fli_feed
        WHERE date_trunc('day', hour) NOT IN (SELECT dt FROM prices_usd)
            AND usd_price IS NOT NULL
        GROUP BY 1
    
    ),
    
    fli_price AS (
    
    SELECT
        *
    FROM prices_usd
    
    UNION ALL
    
    SELECT
        *
    FROM fli_price_feed
    
    )
    
    SELECT
        *
    FROM fli_price
    WHERE dt > '2021-03-14'
    ORDER BY 1

),

eth2x_days AS (
    
    SELECT generate_series('2021-03-14'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
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
FROM eth2x_days d
LEFT JOIN mint_volume m ON d.day = m.day
LEFT JOIN redeem_volume r ON d.day = r.day
LEFT JOIN eth2x_daily_price_feed p ON d.day = p.dt