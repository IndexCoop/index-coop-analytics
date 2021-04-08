-- https://duneanalytics.com/queries/27989/56554

-- CGI Supply Breakdown

WITH cgi_uniswap_pairs AS (

  SELECT
    token0,
    18 as decimals0,
    'CGI' as symbol0,
    token1,
    18 as decimals1,
    'wETH' as symbol1,
    pair
  FROM uniswap_v2."Factory_evt_PairCreated" pairsraw
  WHERE pair = '\x3458766bfd015df952ddb286fe315d58ecf6f516'
  
),

cgi_uniswap_reserves AS (

  SELECT
    AVG(s.reserve0 / 10^p.decimals0) AS reserve0,
    AVG(s.reserve1 / 10^p.decimals1) AS reserve1,
    s.contract_address,
    date_trunc('day', s.evt_block_time) AS dt,
    p.symbol0,
    p.symbol1
  FROM uniswap_v2."Pair_evt_Sync" s
  JOIN cgi_uniswap_pairs p ON s.contract_address = p.pair
  GROUP BY 3, 4, 5, 6

),

cgi_uniswap_supply AS (

    SELECT
        SUM(CASE
            WHEN symbol0 = 'CGI' THEN reserve0
            WHEN symbol1 = 'CGI' THEN reserve1
            ELSE NULL
        END) AS reserves,
        dt,
        'CGI' AS product,
        'uniswap' AS project
    FROM cgi_uniswap_reserves
    GROUP BY 2, 3, 4
 
),

cgi_liquidity_supply_temp AS (

SELECT dt, reserves FROM cgi_uniswap_supply

),

cgi_liquidity_supply_temp2 AS (

    SELECT
        dt,
        SUM(reserves) AS reserves
    FROM cgi_liquidity_supply_temp
    GROUP BY 1

),

cgi_days AS (
    
    SELECT generate_series('2021-02-11 00:00:00'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

cgi_liquidity_supply AS (

SELECT
    d.day AS dt,
    (ARRAY_REMOVE(ARRAY_AGG(i.reserves) OVER (ORDER BY d.day), NULL))[COUNT(i.reserves) OVER (ORDER BY d.day)] AS reserves
FROM cgi_days d
LEFT JOIN cgi_liquidity_supply_temp2 i ON d.day = i.dt

),

cgi_mint_burn AS (

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

cgi_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM cgi_days d
    LEFT JOIN cgi_mint_burn m ON d.day = m.day
    
),

cgi_total_supply AS (

SELECT 
    day, 
    SUM(amount) OVER (ORDER BY day) AS cgi
FROM cgi_units

),

--cgi price feed
cgi_swap AS (

--eth/cgi uni        x3458766bfd015df952ddb286fe315d58ecf6f516
    
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour,
        ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
        ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM uniswap_v2."Pair_evt_Swap" sw
    WHERE contract_address = '\x3458766bfd015df952ddb286fe315d58ecf6f516' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '202-02-11'

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

cgi_price_feed AS (

SELECT
    date_trunc('day', hour) AS dt,
    AVG(usd_price) AS price
FROM cgi_feed
WHERE usd_price IS NOT NULL
GROUP BY 1

)

SELECT
    DISTINCT
    t.day,
    'CGI' AS product,
    t.cgi AS total,
    0 AS incentivized,
    t.cgi - 0 AS unincentivized,
    l.reserves AS liquidity,
    t.cgi * p.price AS tvl,
    0 * p.price AS itvl,
    (t.cgi - 0) * p.price AS utvl,
    l.reserves * p.price AS liquidity_value,
    p.price
FROM cgi_total_supply t
LEFT JOIN cgi_liquidity_supply l ON t.day = l.dt
LEFT JOIN cgi_price_feed p ON t.day = p.dt
WHERE t.day >= '2021-02-11'