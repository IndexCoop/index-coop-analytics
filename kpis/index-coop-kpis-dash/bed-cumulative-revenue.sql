-- https://duneanalytics.com/queries/89117

WITH bed_mint_burn AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\x2af1df3ab0ab157e1e2ad8f88a7d04fbea0c7dc6'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed" 
    WHERE "_setToken" = '\x2af1df3ab0ab157e1e2ad8f88a7d04fbea0c7dc6'
    GROUP BY 1
),

bed_days AS (
    
    SELECT generate_series('2021-07-21'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

bed_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM bed_days d
    LEFT JOIN bed_mint_burn m ON d.day = m.day
    
),

bed AS (

SELECT 
    day,
    'BED' AS product,
    SUM(amount) OVER (ORDER BY day) AS units
FROM bed_units

),

bed_swap AS (

    SELECT 
        date_trunc('hour', u.evt_block_time) AS hour,
        AVG((ABS(amount1) / 1e18) / (ABS(amount0) / 1e18) * p.price) AS bed_usd
    FROM uniswap_v3."Pair_evt_Swap" u
    LEFT JOIN (SELECT * FROM prices.usd WHERE symbol = 'WETH') p ON date_trunc('minute', u.evt_block_time) = p.minute
    WHERE u.contract_address = '\x779DFffB81550BF503C19d52b1e91e9251234fAA'
    GROUP BY 1

),

bed_hours AS (
    
    SELECT generate_series('2021-07-21 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
    
),

bed_temp AS (

SELECT
    h.hour,
    COALESCE(s.bed_usd, NULL) AS usd_price
FROM bed_hours h
LEFT JOIN bed_swap s ON s."hour" = h.hour 

),

bed_feed AS (

SELECT
    hour,
    'BED' AS product,
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price
FROM bed_temp

),

bed_aum AS (

SELECT
    d.*,
    COALESCE(p.price, f.usd_price) AS price,
    COALESCE(p.price * d.units, f.usd_price * d.units) AS aum
FROM bed d
LEFT JOIN prices.usd p ON p.symbol = d.product AND d.day = p.minute
LEFT JOIN bed_feed f ON f.product = d.product AND d.day = f.hour

),

bed_daily_revenue AS (

SELECT
    DISTINCT
    *,
    aum * (.00125/365) AS daily_revenue
FROM bed_aum

)

SELECT 
    *,
    SUM(daily_revenue) OVER (ORDER BY day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS revenue
FROM bed_daily_revenue

-- ,
--     aum * (.00125/365),
--     SUM(aum * (.00125/365)) OVER (ORDER BY day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS revenue