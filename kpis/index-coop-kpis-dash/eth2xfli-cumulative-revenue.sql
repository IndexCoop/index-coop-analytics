-- https://duneanalytics.com/queries/26502/53792

WITH fli_mint_burn AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenRedeemed" 
    WHERE "_setToken" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    GROUP BY 1
    
),

fli_days AS (
    
    SELECT generate_series('2021-03-13'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
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
    'ETH2X-FLI' AS product,
    SUM(amount) OVER (ORDER BY day) AS units
FROM fli_units

),

fli_swap AS (

--eth/fli uni        xf91c12dae1313d0be5d7a27aa559b1171cc1eac5
    
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour,
        ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
        ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM uniswap_v2."Pair_evt_Swap" sw
    WHERE contract_address = '\xf91c12dae1313d0be5d7a27aa559b1171cc1eac5' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2021-03-14'

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
ORDER BY 1

),

fli_feed AS (

SELECT
    hour,
    'ETH2X-FLI' AS product,
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
    (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
FROM fli_temp

),

-- SELECT
--     *
-- FROM fli_feed
-- WHERE usd_price IS NOT NULL

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
    SUM(ABS(amount)) AS mint_burn_amount
FROM fli_mint_burn
GROUP BY 1

),

fli_mint_burn_fee AS (

    SELECT
        a.*,
        a.mint_burn_amount * b.usd_price AS mint_burn_dollars,
        a.mint_burn_amount * b.usd_price * .0006 AS revenue
    FROM fli_mint_burn_amount a
    LEFT JOIN fli_feed b ON a.day = b.hour

),

fli_revenue_temp AS (

    SELECT
        DISTINCT
        a.*,
        -- a.aum * .0117/365 AS streaming_revenue,
        -- b.revenue AS mint_burn_revenue,
        (a.aum * .0117/365) + COALESCE(b.revenue, 0) AS revenue
    FROM fli_aum a
    LEFT JOIN fli_mint_burn_fee b ON a.day = b.day
    ORDER BY 1
    
),

fli_revenue AS (

    SELECT
        day,
        product,
        units,
        price,
        aum,
        SUM(revenue) OVER (ORDER BY day) AS revenue
    FROM fli_revenue_temp

)

SELECT * FROM fli_revenue
