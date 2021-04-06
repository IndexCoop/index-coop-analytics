-- https://duneanalytics.com/queries/26508/53800

WITH cgi_mint_burn AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed" 
    WHERE "_setToken" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'
    GROUP BY 1

),

cgi_days AS (
    
    SELECT generate_series('2021-02-10'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

cgi_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM cgi_days d
    LEFT JOIN cgi_mint_burn m ON d.day = m.day
    
),

cgi AS (

SELECT 
    day,
    'CGI' AS product,
    SUM(amount) OVER (ORDER BY day) AS units
FROM cgi_units

),

cgi_swap AS (

--eth/cgi uni        x3458766bfd015df952ddb286fe315d58ecf6f516
    
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour,
        ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
        ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM uniswap_v2."Pair_evt_Swap" sw
    WHERE contract_address = '\x3458766bfd015df952ddb286fe315d58ecf6f516' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2021-02-11'

),

cgi_a1_prcs AS (

    SELECT 
        avg(price) a1_prc, 
        date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2021-02-11'
        AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --weth as base asset
    GROUP BY 2
                
),

cgi_hours AS (
    
    SELECT generate_series('2021-02-11 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
    
),

cgi_temp AS (

SELECT
    h.hour,
    COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price, 
    COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as eth_price
    -- a1_prcs."minute" AS minute
FROM cgi_hours h
LEFT JOIN cgi_swap s ON s."hour" = h.hour 
LEFT JOIN cgi_a1_prcs a ON h."hour" = a."hour"
GROUP BY 1

),

cgi_feed AS (

SELECT
    hour,
    'CGI' AS product,
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
    (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
FROM cgi_temp

),

cgi_aum AS (

SELECT
    d.*,
    COALESCE(p.price, f.usd_price) AS price,
    COALESCE(p.price * d.units, f.usd_price * d.units) AS aum
FROM cgi d
LEFT JOIN prices.usd p ON p.symbol = d.product AND d.day = p.minute
LEFT JOIN cgi_feed f ON f.product = d.product AND d.day = f.hour

),

cgi_revenue AS (

SELECT
    DISTINCT
    *,
    SUM(aum * .0024/365) OVER (ORDER BY day) AS revenue
FROM cgi_aum

)

SELECT * FROM cgi_revenue