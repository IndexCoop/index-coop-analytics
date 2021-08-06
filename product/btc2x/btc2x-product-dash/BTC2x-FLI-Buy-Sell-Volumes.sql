WITH btc2x_daily_price_feed AS (

    WITH prices_usd AS (
    
        SELECT
            date_trunc('day', minute) AS dt,
            AVG(price) AS price
        FROM prices.usd
        WHERE symbol = 'BTC2x-FLI'
        GROUP BY 1
        ORDER BY 1
        
    ),
    
    fli_swap AS (
    
    --eth/fli uni        xf91c12dae1313d0be5d7a27aa559b1171cc1eac5
    ----btc2x/wbtc sushi 'x164FE0239d703379Bddde3c80e4d4800A1cd452B'    
        select 
    date_trunc('hour', sw."evt_block_time") AS hour,
        ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
        ("amount1In" + "amount1Out")/1e8 AS a1_amt
    from sushi."Pair_evt_Swap" sw
    where contract_address = '\x164FE0239d703379Bddde3c80e4d4800A1cd452B'
    AND sw.evt_block_time >= '2021-03-14' -- 
    ),
    
    fli_a1_prcs AS (
    
        SELECT 
            avg(price) a1_prc, 
            date_trunc('hour', minute) AS hour
        FROM prices.usd
        WHERE minute >= '2021-03-12'
            AND contract_address ='\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599' --wbtc as base asset
        GROUP BY 2
                    
    ),
    
    fli_hours AS (
        
        SELECT generate_series('2021-05-01 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
        
    ),
    
    fli_temp AS (
    
    SELECT
        h.hour,
        COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price, 
        COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as btc_price
        -- a1_prcs."minute" AS minute
    FROM fli_hours h
    LEFT JOIN fli_swap s ON s."hour" = h.hour 
    LEFT JOIN fli_a1_prcs a ON h."hour" = a."hour"
    GROUP BY 1
    
    ),
    
    fli_feed AS (
    
    SELECT
        hour,
        'BTC2x-FLI' AS product,
        (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
        (ARRAY_REMOVE(ARRAY_AGG(btc_price) OVER (ORDER BY hour), NULL))[COUNT(btc_price) OVER (ORDER BY hour)] AS btc_price
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
    WHERE dt > '2021-05-01'
    ORDER BY 1

),

buys AS (

    SELECT DISTINCT ON (tx_hash, trace_address, evt_index)
        'BTC2x-FLI' AS product,
         date_trunc('day', block_time) as day,
        'buy' AS tx,
        token_a_amount AS amount,
        p.price,
        token_a_amount * p.price AS usd_volume,
        tx_hash
    FROM dex.trades t
    INNER JOIN btc2x_daily_price_feed p
    ON date_trunc('day', block_time) = p.dt
    WHERE token_a_address = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'

),

sells AS (

    SELECT DISTINCT ON (tx_hash, trace_address, evt_index)
        'BTC2x-FLI' AS product,
         date_trunc('day', block_time) as day,
        'sell' AS tx,
        token_b_amount AS amount,
        p.price,
        token_b_amount * p.price AS usd_volume,
        tx_hash
    FROM dex.trades t
    INNER JOIN btc2x_daily_price_feed p
    ON date_trunc('day', block_time) = p.dt
    WHERE token_b_address = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'

),

buys_sells AS (

SELECT * FROM buys

UNION ALL

SELECT * FROM sells

)

SELECT
    product,
    day,
    COUNT(*) FILTER (WHERE tx = 'buy') as buys, 
    COUNT(*) FILTER (WHERE tx = 'sell') as sells,
    SUM(
        CASE
            WHEN tx = 'buy' THEN 1
            WHEN tx = 'sell' THEN -1
            ELSE NULL
        END
    ) AS net,
    SUM(amount * price) FILTER (WHERE tx = 'buy') AS buy_volume,
    SUM(amount * price) FILTER (WHERE tx = 'sell') AS sell_volume,
    SUM(
        CASE
            WHEN tx = 'buy' THEN amount * price
            WHEN tx = 'sell' THEN -amount  * price
            ELSE NULL
        END
    ) AS net_volume,
    SUM(amount * price) AS total_volume
FROM buys_sells
GROUP BY 1, 2
ORDER BY 2 
