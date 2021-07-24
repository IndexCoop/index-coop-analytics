-- https://duneanalytics.com/queries/89041

WITH bed_swap AS (

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

)

SELECT
    *
FROM bed_feed
WHERE usd_price IS NOT NULL



