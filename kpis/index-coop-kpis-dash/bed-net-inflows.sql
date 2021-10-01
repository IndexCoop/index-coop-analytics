-- https://dune.xyz/queries/189856

WITH

-- QUERY: https://dune.xyz/queries/142697/313734
    -- index_products
    -- days
    -- days_and_tokens
    -- hours
    -- hours_and_tokens
    -- std_mint
    -- std_burn
    -- std_issuance
    -- fli_mint
    -- fli_burn
    -- fli_issuance
    -- issuance
    -- all_issuance
    -- v2swaps
    -- v3swaps
    -- sushi_swaps
    -- weth_prices
    -- wbtc_prices
    -- hourly_usd_prices
    -- daily_usd_prices
    -- aum
    -- moving_avg
    
index_products AS (
SELECT * FROM dune_user_generated.index_products
),

days AS (
SELECT 
    generate_series(min(p.inception_date)::timestamp, 
    date_trunc('day', NOW()), '1 day') as day
FROM index_products p
),

days_and_tokens AS (
SELECT 
    d.day,
    p.token_address AS token_address,
    p.name AS name,
    p.index_type AS index_type
FROM days d
CROSS JOIN index_products p
),

hours AS (
SELECT
    generate_series(min(p.inception_date)::timestamp,
    date_trunc('hour', NOW()), '1 hour') AS hour
FROM index_products p
),

hours_and_tokens AS (
SELECT 
    h.hour,
    p.token_address AS token_address,
    p.name AS name,
    p.index_type AS index_type,
    p.swap_address,
    p.swap_type,
    p.swap_base
FROM hours h
CROSS JOIN index_products p
),

std_mint AS (
SELECT 
    date_trunc('day', evt_block_time) AS day, 
    "_setToken" AS token_address,
    SUM("_quantity"/1e18) AS amount
FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
WHERE "_setToken" IN (SELECT token_address FROM index_products WHERE index_type = 'Standard')
GROUP BY 1,2
),

std_burn AS (
SELECT 
    date_trunc('day', evt_block_time) AS day,
    "_setToken" AS token_address,
    SUM("_quantity"/1e18) AS amount
FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed"
WHERE "_setToken" IN (SELECT token_address FROM index_products WHERE index_type = 'Standard')
GROUP BY 1,2
),

std_issuance AS (
SELECT 
    d.day,
    d.token_address,
    COALESCE(m.amount, 0) AS mint_amount,
    COALESCE(b.amount, 0) AS burn_amount,
    COALESCE(m.amount, 0) - COALESCE(b.amount, 0) AS net_issue_amount
FROM days_and_tokens d
LEFT JOIN std_mint m ON d.day = m.day AND d.token_address = m.token_address
LEFT JOIN std_burn b ON d.day = b.day AND d.token_address = b.token_address
),

fli_mint AS (
SELECT 
    date_trunc('day', evt_block_time) AS day,
    "_setToken" AS token_address,
    SUM("_quantity"/1e18) AS amount
FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenIssued"
WHERE "_setToken" IN (SELECT token_address FROM index_products WHERE index_type = 'Leveraged')
GROUP BY 1,2
),

fli_burn AS (
    SELECT 
        date_trunc('day', evt_block_time) AS day,
        "_setToken" AS token_address,
        SUM("_quantity"/1e18) AS amount
    FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenRedeemed"
    WHERE "_setToken" IN (SELECT token_address FROM index_products WHERE index_type = 'Leveraged')
    GROUP BY 1,2
),

fli_issuance AS (
SELECT 
    d.day,
    d.token_address,
    COALESCE(m.amount, 0) AS mint_amount,
    COALESCE(b.amount, 0) AS burn_amount,
    COALESCE(m.amount, 0) - COALESCE(b.amount, 0) AS net_issue_amount
FROM days_and_tokens d
LEFT JOIN fli_mint m ON d.day = m.day AND d.token_address = m.token_address
LEFT JOIN fli_burn b ON d.day = b.day AND d.token_address  = b.token_address
),

issuance AS (
SELECT 
    d.day,
    d.token_address,
    d.name,
    CASE    WHEN d.index_type = 'Standard' THEN s.mint_amount
            WHEN d.index_type = 'Leveraged' THEN f.mint_amount
            ELSE 0
    END AS mint_amount,
    CASE    WHEN d.index_type = 'Standard' THEN s.burn_amount
            WHEN d.index_type = 'Leveraged' THEN f.burn_amount
            ELSE 0
    END AS burn_amount,
    CASE    WHEN d.index_type = 'Standard' THEN s.mint_amount - s.burn_amount
            WHEN d.index_type = 'Leveraged' THEN f.mint_amount - f.burn_amount
            ELSE 0
    END AS net_issue_amount
FROM days_and_tokens d
LEFT JOIN std_issuance s ON s.token_address = d.token_address AND s.day = d.day
LEFT JOIN fli_issuance f ON f.token_address = d.token_address AND f.day = d.day
),

all_issuance AS (
SELECT
    *,
    sum(net_issue_amount) OVER (PARTITION BY token_address ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS units
FROM issuance
),

v2swaps AS (
SELECT
    date_trunc('hour', s1."evt_block_time") AS hour,
    (s1."amount1In" + s1."amount1Out")/(s1."amount0In" + s1."amount0Out") AS swap_price,
    s1.contract_address AS swap_address
FROM    uniswap_v2."Pair_evt_Swap" s1
WHERE   contract_address IN (SELECT swap_address FROM index_products WHERE swap_type = 'UniswapV2')
),

v3swaps AS (
SELECT
    date_trunc('hour', s2."evt_block_time") AS hour,
    AVG((ABS(s2.amount1)) / (ABS(s2.amount0))) AS swap_price,
    s2.contract_address AS swap_address
FROM    uniswap_v3."Pair_evt_Swap" s2
WHERE   contract_address IN (SELECT swap_address FROM index_products WHERE swap_type = 'UniswapV3')
GROUP BY 1,3
),

sushi_swaps AS (
SELECT
    date_trunc('hour', s3."evt_block_time") AS hour,
    ((s3."amount1In" + s3."amount1Out"))/(s3."amount0In" + s3."amount0Out") AS swap_price,
    s3.contract_address AS swap_address
FROM    sushi."Pair_evt_Swap" s3
WHERE   contract_address IN (SELECT swap_address FROM index_products WHERE swap_type = 'Sushi')
),

weth_prices AS (
SELECT 
    avg(price) weth_price, 
    date_trunc('hour', minute) AS hour
FROM prices.usd
WHERE minute >= (SELECT MIN(inception_date) FROM index_products)
AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
AND minute >= '2020-09-10'
GROUP BY 2
),

wbtc_prices AS (
SELECT 
    avg(price)*1e10 AS wbtc_price, 
    date_trunc('hour', minute) AS hour
FROM prices.usd
WHERE minute >= (SELECT MIN(inception_date) FROM index_products)
AND contract_address ='\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
AND minute >= '2021-05-11'
GROUP BY 2
),

hourly_usd_prices AS (
SELECT
    h.hour,
    h.token_address,
    CASE    WHEN    h.swap_type = 'UniswapV2' AND h.swap_base = 'ETH' then s1.swap_price * e.weth_price
            WHEN    h.swap_type = 'UniswapV3' AND h.swap_base = 'ETH' then s2.swap_price * e.weth_price
            WHEN    h.swap_type = 'Sushi' AND h.swap_base = 'ETH' then s3.swap_price * e.weth_price
            WHEN    h.swap_type = 'UniswapV2' AND h.swap_base = 'BTC' then s1.swap_price * b.wbtc_price
            WHEN    h.swap_type = 'UniswapV3' AND h.swap_base = 'BTC' then s2.swap_price * b.wbtc_price
            WHEN    h.swap_type = 'Sushi' AND h.swap_base = 'BTC' then s3.swap_price * b.wbtc_price
    END AS token_usd_price 
FROM hours_and_tokens h
LEFT JOIN v2swaps s1 ON h."hour" = s1.hour AND h.swap_address = s1.swap_address
LEFT JOIN v3swaps s2 ON h."hour" = s2.hour AND h.swap_address = s2.swap_address
LEFT JOIN sushi_swaps s3 ON h."hour" = s3.hour AND h.swap_address = s3.swap_address
LEFT JOIN weth_prices e ON h."hour" = e.hour
LEFt JOIN wbtc_prices b ON h."hour" = b.hour
GROUP BY 1,2,3
),

daily_usd_prices AS (
SELECT
    d.day,
    d.token_address,
    AVG(h.token_usd_price) AS price
FROM days_and_tokens d
LEFT JOIN hourly_usd_prices h ON d.day = date_trunc('day', h.hour) AND d.token_address = h.token_address
GROUP BY 1,2
),

aum AS (
SELECT
    i.day,
    i.token_address,
    i.name,
    i.units * d.price AS aum,
    i.net_issue_amount * d.price AS net_inflow
FROM all_issuance i
LEFT JOIN daily_usd_prices d ON i.day = d.day AND i.token_address = d.token_address
GROUP BY 1,2,3,4,5
),

moving_avg AS (
SELECT 
    *,
    AVG(net_inflow) OVER (PARTITION BY name ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS seven_day_ma,
    AVG(net_inflow) OVER (PARTITION BY name ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS twenty_eight_day_ma,
    AVG(net_inflow) OVER (PARTITION BY name ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) / AVG(aum) OVER (PARTITION BY name ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS twenty_eight_day_ma_aum
FROM aum
)

SELECT 
    *
FROM moving_avg
WHERE name = 'Bankless Index' AND net_inflow IS NOT NULL