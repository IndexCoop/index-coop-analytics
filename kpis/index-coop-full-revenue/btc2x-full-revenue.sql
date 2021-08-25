-- https://dune.xyz/queries/121580

-- REVENUE
WITH fli_mint_burn AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day,
        'mint' AS action,
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day,
        'redeem' AS action,
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenRedeemed" 
    WHERE "_setToken" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
    GROUP BY 1
    
),

fli_days AS (
    
    SELECT generate_series('2021-05-07'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

fli_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM fli_days d
    LEFT JOIN fli_mint_burn m ON d.day = m.day
    
),

fli AS (

SELECT 
    day,
    'BTC2X-FLI' AS product,
    SUM(amount) OVER (ORDER BY day) AS units
FROM fli_units

),

fli_swap AS (

--eth/fli uni        xf91c12dae1313d0be5d7a27aa559b1171cc1eac5
--btc2x/wbtc sushi 'x164FE0239d703379Bddde3c80e4d4800A1cd452B'
    
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
    
    SELECT generate_series('2021-05-07 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
    
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
ORDER BY 1

),

fli_feed AS (

SELECT
    hour,
    'BTC2X-FLI' AS product,
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
    (ARRAY_REMOVE(ARRAY_AGG(btc_price) OVER (ORDER BY hour), NULL))[COUNT(btc_price) OVER (ORDER BY hour)] AS btc_price
FROM fli_temp

),

fli_aum AS (

SELECT
    d.*,
    f.usd_price AS price,
    f.usd_price * d.units AS aum
FROM fli d
LEFT JOIN fli_feed f ON f.product = d.product AND d.day = f.hour

),

fli_mint_burn_amount AS (

SELECT
    day,
    SUM(ABS(amount)) AS amount
FROM fli_mint_burn
GROUP BY 1

),

fli_mint_burn_revenue AS (

    SELECT
        a.*,
        a.amount * b.usd_price * .001 AS revenue
    FROM fli_mint_burn_amount a
    LEFT JOIN fli_feed b ON a.day = b.hour

),

fli_revenue AS (

    SELECT
        DISTINCT
        a.day,
        'revenue' AS detail,
        (a.aum * .0195/365) AS streaming,
        COALESCE(b.revenue, 0) AS mint_redeem,
        (a.aum * .0195/365) + COALESCE(b.revenue, 0) AS total_revenue
    FROM fli_aum a
    LEFT JOIN fli_mint_burn_revenue b ON a.day = b.day
    
),

revenue AS (

SELECT 
    *,
    SUM(streaming) OVER (ORDER BY day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_streaming,
    SUM(mint_redeem) OVER (ORDER BY day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_mint_redeem,
    SUM(total_revenue) OVER (ORDER BY day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_total_revenue
FROM fli_revenue
ORDER BY 1

)

SELECT * FROM revenue