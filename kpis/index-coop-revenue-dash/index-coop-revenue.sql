-- https://duneanalytics.com/queries/46934

WITH dpi_mint_burn AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed" 
    WHERE "_setToken" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    GROUP BY 1
),

dpi_days AS (
    
    SELECT generate_series('2020-09-10'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

dpi_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM dpi_days d
    LEFT JOIN dpi_mint_burn m ON d.day = m.day
    
),

dpi AS (

SELECT 
    day,
    'DPI' AS product,
    'DeFi Pulse' AS methodologist,
    SUM(amount) OVER (ORDER BY day) AS units
FROM dpi_units

),

dpi_swap AS (

--eth/dpi uni        x4d5ef58aac27d99935e5b6b4a6778ff292059991
    
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour,
        ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
        ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM uniswap_v2."Pair_evt_Swap" sw
    WHERE contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2020-09-10'

),

dpi_a1_prcs AS (

    SELECT 
        avg(price) a1_prc, 
        date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2020-09-10'
        AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --weth as base asset
    GROUP BY 2
                
),

dpi_hours AS (
    
    SELECT generate_series('2020-09-10 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
    
),

dpi_temp AS (

SELECT
    h.hour,
    COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price, 
    COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as eth_price
    -- a1_prcs."minute" AS minute
FROM dpi_hours h
LEFT JOIN dpi_swap s ON s."hour" = h.hour 
LEFT JOIN dpi_a1_prcs a ON h."hour" = a."hour"
GROUP BY 1

),

dpi_feed AS (

SELECT
    hour,
    'DPI' AS product,
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price
    -- (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
FROM dpi_temp

),

dpi_aum AS (

SELECT
    d.*,
    COALESCE(p.price, f.usd_price) AS price,
    COALESCE(p.price * d.units, f.usd_price * d.units) AS aum
FROM dpi d
LEFT JOIN prices.usd p ON p.symbol = d.product AND d.day = p.minute
LEFT JOIN dpi_feed f ON f.product = d.product AND d.day = f.hour

),

dpi_revenue AS (

SELECT
    DISTINCT
    *,
    aum * .00665/365 AS revenue
FROM dpi_aum

),

-- ETH2x-FLI
fli_mint_burn AS (

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
    'ETH2x-FLI' AS product,
    'DeFi Pulse' AS methodologist,
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
    'ETH2x-FLI' AS product,
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price
    -- (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
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

fli_revenue AS (

    SELECT
        DISTINCT
        a.*,
        -- a.aum * .0117/365 AS streaming_revenue,
        -- b.revenue AS mint_burn_revenue,
        (a.aum * .0117/365) + b.revenue AS revenue
    FROM fli_aum a
    LEFT JOIN fli_mint_burn_fee b ON a.day = b.day
    ORDER BY 1
    
),

-- BTC2x-FLI
btc2x_revenue AS (

    WITH fli_mint_burn AS (
    
        SELECT 
            date_trunc('day', evt_block_time) AS day, 
            SUM("_quantity"/1e18) AS amount 
            FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenIssued"
            WHERE "_setToken" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
            GROUP BY 1
    
        UNION ALL
    
        SELECT 
            date_trunc('day', evt_block_time) AS day, 
            -SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenRedeemed" 
        WHERE "_setToken" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
        GROUP BY 1
        
    ),
    
    fli_days AS (
        
        SELECT generate_series('2021-05-11'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
        
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
        'BTC2x-FLI' AS product,
        'DeFi Pulse' AS methodologist,
        SUM(amount) OVER (ORDER BY day) AS units
    FROM fli_units
    
    ),
    
    fli_swap AS (
    
    -- btc2x/wbtc sushi x164fe0239d703379bddde3c80e4d4800a1cd452b
        
        SELECT
            date_trunc('hour', sw."evt_block_time") AS hour,
            ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
            ("amount1In" + "amount1Out")/1e8 AS a1_amt
        FROM sushi."Pair_evt_Swap" sw
        WHERE contract_address = '\x164fe0239d703379bddde3c80e4d4800a1cd452b' -- liq pair address I am searching the price for
            AND sw.evt_block_time >= '2021-05-11'
    
    ),
    
    fli_a1_prcs AS (
    
        SELECT 
            avg(price) a1_prc, 
            date_trunc('hour', minute) AS hour
        FROM prices.usd
        WHERE minute >= '2021-05-11'
            AND contract_address ='\x2260fac5e5542a773aa44fbcfedf7c193bc2c599' --wbtc as base asset
        GROUP BY 2
                    
    ),
    
    fli_hours AS (
        
        SELECT generate_series('2021-05-11 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
        
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
        (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price
        -- (ARRAY_REMOVE(ARRAY_AGG(btc_price) OVER (ORDER BY hour), NULL))[COUNT(btc_price) OVER (ORDER BY hour)] AS btc_price
    FROM fli_temp
    
    ),
    
    fli_price_feed AS (
    
    SELECT
        date_trunc('day', hour) AS dt,
        'BTC2x-FLI' AS product,
        AVG(usd_price) AS price
    FROM fli_feed
    WHERE usd_price IS NOT NULL
    GROUP BY 1, 2
    
    ),
    
    fli_aum AS (
    
    SELECT
        d.*,
        f.price AS price,
        f.price * d.units AS aum
    FROM fli d
    LEFT JOIN fli_price_feed f ON f.product = d.product AND d.day = f.dt
    
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
            a.mint_burn_amount * b.price AS mint_burn_dollars,
            a.mint_burn_amount * b.price * .0006 AS revenue
        FROM fli_mint_burn_amount a
        LEFT JOIN fli_price_feed b ON a.day = b.dt
    
    ),
    
    fli_revenue AS (
    
        SELECT
            DISTINCT
            a.*,
            -- a.aum * .0117/365 AS streaming_revenue,
            -- b.revenue AS mint_burn_revenue,
            (a.aum * .0117/365) + b.revenue AS revenue
        FROM fli_aum a
        LEFT JOIN fli_mint_burn_fee b ON a.day = b.day
        ORDER BY 1
        
    )
    
    SELECT 
        *
    FROM fli_revenue

),

mvi_mint_burn AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed" 
    WHERE "_setToken" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
    GROUP BY 1
),

mvi_days AS (
    
    SELECT generate_series('2021-04-06'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

mvi_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM mvi_days d
    LEFT JOIN mvi_mint_burn m ON d.day = m.day
    
),

mvi AS (

SELECT 
    day,
    'MVI' AS product,
    'Index Coop' AS methodologist,
    SUM(amount) OVER (ORDER BY day) AS units
FROM mvi_units

),

mvi_swap AS (

--eth/mvi uni        x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff
    
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour,
        ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
        ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM uniswap_v2."Pair_evt_Swap" sw
    WHERE contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2021-04-07'

),

mvi_a1_prcs AS (

    SELECT 
        avg(price) a1_prc, 
        date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2021-04-07'
        AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --weth as base asset
    GROUP BY 2
                
),

mvi_hours AS (
    
    SELECT generate_series('2021-04-07 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
    
),

mvi_temp AS (

SELECT
    h.hour,
    COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price, 
    COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as eth_price
    -- a1_prcs."minute" AS minute
FROM mvi_hours h
LEFT JOIN mvi_swap s ON s."hour" = h.hour 
LEFT JOIN mvi_a1_prcs a ON h."hour" = a."hour"
GROUP BY 1

),

mvi_feed AS (

SELECT
    hour,
    'MVI' AS product,
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price
    -- (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
FROM mvi_temp

),

mvi_aum AS (

SELECT
    d.*,
    COALESCE(p.price, f.usd_price) AS price,
    COALESCE(p.price * d.units, f.usd_price * d.units) AS aum
FROM mvi d
LEFT JOIN prices.usd p ON p.symbol = d.product AND d.day = p.minute
LEFT JOIN mvi_feed f ON f.product = d.product AND d.day = f.hour

),

mvi_revenue AS (

SELECT
    DISTINCT
    *,
    aum * .0095/365 AS revenue
FROM mvi_aum
WHERE price IS NOT NULL

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
    'Coinshares' AS methodologist,
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
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price
    -- (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
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
    aum * .0024/365 AS revenue
FROM cgi_aum
WHERE day <= '2021-04-30'

),

bed_mint_burn AS (

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
    'Bankless' AS methodologist,
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

bed_revenue AS (

SELECT
    DISTINCT
    *,
    aum * .00125/365 AS revenue
FROM bed_aum

)

SELECT DISTINCT  * FROM dpi_revenue

UNION ALL

SELECT DISTINCT * FROM fli_revenue

UNION ALL

SELECT * FROM btc2x_revenue

UNION ALL 

SELECT DISTINCT * FROM mvi_revenue

UNION ALL 

SELECT DISTINCT * FROM cgi_revenue

UNION ALL

SELECT DISTINCT * FROM bed_revenue