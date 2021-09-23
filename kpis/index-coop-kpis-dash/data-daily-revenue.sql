-- https://dune.xyz/queries/163284

WITH data_mint_burn AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\x33d63ba1e57e54779f7ddaeaa7109349344cf5f1'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed" 
    WHERE "_setToken" = '\x33d63ba1e57e54779f7ddaeaa7109349344cf5f1'
    GROUP BY 1
),

data_days AS (
    
    SELECT generate_series('2021-09-21'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

data_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM data_days d
    LEFT JOIN data_mint_burn m ON d.day = m.day
    
),

data AS (

    SELECT 
        day,
        'DATA' AS product,
        '\x33d63ba1e57e54779f7ddaeaa7109349344cf5f1'::bytea AS contract_address,
        SUM(amount) OVER (ORDER BY day) AS units
    FROM data_units

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
        (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
        (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
    FROM data_temp

),

data_aum AS (

SELECT
    d.*,
    COALESCE(p.price, f.usd_price) AS price,
    COALESCE(p.price * d.units, f.usd_price * d.units) AS aum
FROM data d
LEFT JOIN prices.usd p ON p.contract_address = d.contract_address AND d.day = p.minute
LEFT JOIN data_feed f ON f.product = d.product AND d.day = f.hour

),

data_revenue AS (

SELECT
    DISTINCT
    *,
    aum * .0035/365 AS revenue
FROM data_aum

)

SELECT 
    *,
    AVG(revenue) OVER (ORDER BY day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS av
FROM data_revenue