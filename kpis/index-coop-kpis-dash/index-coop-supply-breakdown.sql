-- https://duneanalytics.com/queries/27995/56566

-- DPI Supply Breakdown
WITH dpi_uniswap_pairs AS (

  SELECT
    token0,
    erc20.decimals as decimals0,
    erc20.symbol as symbol0,
    token1,
    erc202.decimals as decimals1,
    erc202.symbol as symbol1,
    pair
  FROM uniswap_v2."Factory_evt_PairCreated" pairsraw
  LEFT JOIN erc20.tokens erc20 ON pairsraw.token0 = erc20.contract_address
  LEFT JOIN erc20.tokens erc202 ON pairsraw.token1 = erc202.contract_address
--   WHERE token0 IN (SELECT DISTINCT contract_address FROM erc20.tokens WHERE decimals > 0)
--     AND token1 IN (SELECT DISTINCT contract_address FROM erc20.tokens WHERE decimals > 0)
  WHERE erc20.symbol = 'DPI' OR
    erc202.symbol = 'DPI'
  
),

dpi_uniswap_reserves AS (

  SELECT
    AVG(s.reserve0 / 10^p.decimals0) AS reserve0,
    AVG(s.reserve1 / 10^p.decimals1) AS reserve1,
    s.contract_address,
    date_trunc('day', s.evt_block_time) AS dt,
    p.symbol0,
    p.symbol1
  FROM uniswap_v2."Pair_evt_Sync" s
  JOIN dpi_uniswap_pairs p ON s.contract_address = p.pair
  GROUP BY 3, 4, 5, 6

),

dpi_uniswap_supply AS (

    SELECT
        SUM(CASE
            WHEN symbol0 = 'DPI' THEN reserve0
            WHEN symbol1 = 'DPI' THEN reserve1
            ELSE NULL
        END) AS reserves,
        dt,
        'DPI' AS product,
        'uniswap' AS project
    FROM dpi_uniswap_reserves
    GROUP BY 2, 3, 4
 
),

dpi_sushi_pairs AS (

  SELECT
    token0,
    erc20.decimals as decimals0,
    erc20.symbol as symbol0,
    token1,
    erc202.decimals as decimals1,
    erc202.symbol as symbol1,
    pair
  FROM sushi."Factory_evt_PairCreated" pairsraw
  LEFT JOIN erc20.tokens erc20 ON pairsraw.token0 = erc20.contract_address
  LEFT JOIN erc20.tokens erc202 ON pairsraw.token1 = erc202.contract_address
--   WHERE token0 IN (SELECT DISTINCT contract_address FROM erc20.tokens WHERE decimals > 0)
--     AND token1 IN (SELECT DISTINCT contract_address FROM erc20.tokens WHERE decimals > 0)
  WHERE erc20.symbol = 'DPI' OR
    erc202.symbol = 'DPI'
  
),

dpi_sushi_reserves AS (

  SELECT
    AVG(s.reserve0 / 10^p.decimals0) AS reserve0,
    AVG(s.reserve1 / 10^p.decimals1) AS reserve1,
    s.contract_address,
    date_trunc('day', s.evt_block_time) AS dt,
    p.symbol0,
    p.symbol1
  FROM sushi."Pair_evt_Sync" s
  JOIN dpi_sushi_pairs p ON s.contract_address = p.pair
  GROUP BY 3, 4, 5, 6

),

dpi_sushi_supply AS (

    SELECT
        SUM(CASE
            WHEN symbol0 = 'DPI' THEN reserve0
            WHEN symbol1 = 'DPI' THEN reserve1
            ELSE NULL
        END) AS reserves,
        dt,
        'DPI' AS product,
        'sushiswap' AS project
    FROM dpi_sushi_reserves
    GROUP BY 2, 3, 4
 
),

dpi_balancer_supply AS (

    SELECT 
        SUM(cumulative_amount / 10^erc20.decimals) AS reserves,
        day AS dt,
        'DPI' AS product,
        'balancer' AS project
    FROM balancer."view_balances" a
    LEFT JOIN erc20.tokens erc20 ON a.token = erc20.contract_address
    LEFT JOIN prices.usd p ON a.token = p.contract_address
        AND p.minute = date_trunc('minute', a.day)
    WHERE token = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    GROUP BY 2, 3, 4

),

dpi_liquidity_supply_temp AS (

SELECT dt, reserves FROM dpi_uniswap_supply

UNION ALL

SELECT dt, reserves FROM dpi_sushi_supply

UNION ALL

SELECT dt, reserves FROM dpi_balancer_supply

),

dpi_liquidity_supply AS (

    SELECT
        dt,
        SUM(reserves) AS reserves
    FROM dpi_liquidity_supply_temp
    GROUP BY 1

),

dpi_lp_w_lm_option_supply AS (

    SELECT dt, reserves FROM dpi_uniswap_supply

),

dpi_mint_burn AS (

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

dpi_total_supply AS (

SELECT 
    day, 
    SUM(amount) OVER (ORDER BY day) AS dpi
FROM dpi_units

),

dpi_mint_burn_lp AS (

  SELECT
    tr."from" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'burn' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
    AND tr."to" = '\x0000000000000000000000000000000000000000'

  UNION ALL

  SELECT
    tr."to" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'mint' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
    AND tr."from" = '\x0000000000000000000000000000000000000000'

),

dpi_mint_burn_lp_temp AS (

SELECT
    evt_block_day,
    SUM(amount) AS lp_amount
FROM dpi_mint_burn_lp
GROUP BY 1
ORDER BY 1

),

dpi_lp AS (

SELECT
    *,
    SUM(lp_amount) OVER (ORDER BY evt_block_day) AS lp_running_amount
FROM dpi_mint_burn_lp_temp

),

dpi_stake_unstake_lm AS (

  SELECT
    tr."from" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'stake' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
    AND tr."to" IN ('\x8f06FBA4684B5E0988F215a47775Bb611Af0F986', '\xB93b505Ed567982E2b6756177ddD23ab5745f309')

  UNION ALL

  SELECT
    tr."to" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'unstake' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
    AND tr."from" IN ('\x8f06FBA4684B5E0988F215a47775Bb611Af0F986', '\xB93b505Ed567982E2b6756177ddD23ab5745f309')

),

dpi_stake_unstake_lm_temp AS (

SELECT
    evt_block_day,
    SUM(amount) AS lm_amount
FROM dpi_stake_unstake_lm
GROUP BY 1
ORDER BY 1

),

dpi_lm AS (

SELECT
    *,
    SUM(lm_amount) OVER (ORDER BY evt_block_day) AS lm_running_amount
FROM dpi_stake_unstake_lm_temp

),

dpi_lp_lm AS (

    SELECT 
        lp.*,
        COALESCE(lm.lm_amount, 0) AS lm_amount,
        COALESCE(lm.lm_running_amount, 0) AS lm_running_amount,
        COALESCE(lm.lm_running_amount / lp.lp_running_amount, 0) AS perc_lp_lm
    FROM dpi_lp lp
    LEFT JOIN dpi_lm lm USING (evt_block_day)
    
),

dpi_index_rewards AS (

  SELECT
    tr."to" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'reward' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x0954906da0bf32d5479e25f46056d22f08464cab'
    AND tr."from" IN ('\x8f06FBA4684B5E0988F215a47775Bb611Af0F986', '\xB93b505Ed567982E2b6756177ddD23ab5745f309')

),

dpi_daily_index_rewards AS (

SELECT
    evt_block_day,
    SUM(amount) AS amount
FROM dpi_index_rewards
GROUP BY 1
ORDER BY 1

),

dpi_7day_avg_index_rewards AS (

    SELECT
        evt_block_day,
        AVG(amount) OVER (ORDER BY evt_block_day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS index
    FROM dpi_daily_index_rewards
),

dpi AS (

SELECT
    DISTINCT
    t.day,
    'DPI' AS product,
    t.dpi AS total,
    m.reserves * i.perc_lp_lm AS incentivized,
    t.dpi - (m.reserves * i.perc_lp_lm) AS unincentivized,
    l.reserves AS liquidity
FROM dpi_total_supply t
LEFT JOIN dpi_liquidity_supply l ON t.day = l.dt
LEFT JOIN dpi_lp_w_lm_option_supply m ON t.day = m.dt
LEFT JOIN dpi_lp_lm i ON t.day = i.evt_block_day
LEFT JOIN dpi_7day_avg_index_rewards d ON t.day = d.evt_block_day
WHERE t.day >= '2020-10-06'

),

-- ETH2x-FLI Supply Breakdown
fli_uniswap_pairs AS (

  SELECT
    token0,
    18 as decimals0,
    'FLI' as symbol0,
    token1,
    18 as decimals1,
    'wETH' as symbol1,
    pair
  FROM uniswap_v2."Factory_evt_PairCreated" pairsraw
  WHERE pair = '\xf91c12dae1313d0be5d7a27aa559b1171cc1eac5'
--   LEFT JOIN erc20.tokens erc20 ON pairsraw.token0 = erc20.contract_address
--   LEFT JOIN erc20.tokens erc202 ON pairsraw.token1 = erc202.contract_address
-- --   WHERE token0 IN (SELECT DISTINCT contract_address FROM erc20.tokens WHERE decimals > 0)
-- --     AND token1 IN (SELECT DISTINCT contract_address FROM erc20.tokens WHERE decimals > 0)
--   WHERE erc20.symbol = 'DPI' OR
--     erc202.symbol = 'DPI'
  
),

fli_uniswap_reserves AS (

  SELECT
    AVG(s.reserve0 / 10^p.decimals0) AS reserve0,
    AVG(s.reserve1 / 10^p.decimals1) AS reserve1,
    s.contract_address,
    date_trunc('day', s.evt_block_time) AS dt,
    p.symbol0,
    p.symbol1
  FROM uniswap_v2."Pair_evt_Sync" s
  JOIN fli_uniswap_pairs p ON s.contract_address = p.pair
  GROUP BY 3, 4, 5, 6

),

fli_uniswap_supply AS (

    SELECT
        SUM(CASE
            WHEN symbol0 = 'FLI' THEN reserve0
            WHEN symbol1 = 'FLI' THEN reserve1
            ELSE NULL
        END) AS reserves,
        dt,
        'FLI' AS product,
        'uniswap' AS project
    FROM fli_uniswap_reserves
    GROUP BY 2, 3, 4
 
),

fli_liquidity_supply_temp AS (

SELECT dt, reserves FROM fli_uniswap_supply

),

fli_liquidity_supply AS (

    SELECT
        dt,
        SUM(reserves) AS reserves
    FROM fli_liquidity_supply_temp
    GROUP BY 1

),

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

fli_total_supply AS (

SELECT 
    day, 
    SUM(amount) OVER (ORDER BY day) AS fli
FROM fli_units

),
--fli price feed
fli_swap AS (

--eth/fli uni        xf91c12dae1313d0be5d7a27aa559b1171cc1eac5
    
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour,
        ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
        ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM uniswap_v2."Pair_evt_Swap" sw
    WHERE contract_address = '\xf91c12dae1313d0be5d7a27aa559b1171cc1eac5' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2020-03-12'

),

fli_a1_prcs AS (

    SELECT 
        avg(price) a1_prc, 
        date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2021-02-11'
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

),

fli_feed AS (

SELECT
    hour,
    'ETH2x-FLI' AS product,
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
    (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
FROM fli_temp

),

fli_price_feed AS (

SELECT
    *
FROM fli_feed
WHERE usd_price IS NOT NULL

),

fli AS (

SELECT
    DISTINCT
    t.day,
    'ETH2X-FLI' AS product,
    t.fli AS total,
    0 AS incentivized,
    t.fli - 0 AS unincentivized,
    l.reserves AS liquidity
FROM fli_total_supply t
LEFT JOIN fli_liquidity_supply l ON t.day = l.dt
WHERE t.day >= '2021-03-14'

),

-- CGI Supply Breakdown
cgi_uniswap_pairs AS (

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
--   LEFT JOIN erc20.tokens erc20 ON pairsraw.token0 = erc20.contract_address
--   LEFT JOIN erc20.tokens erc202 ON pairsraw.token1 = erc202.contract_address
-- --   WHERE token0 IN (SELECT DISTINCT contract_address FROM erc20.tokens WHERE decimals > 0)
-- --     AND token1 IN (SELECT DISTINCT contract_address FROM erc20.tokens WHERE decimals > 0)
--   WHERE erc20.symbol = 'DPI' OR
--     erc202.symbol = 'DPI'
  
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

c_liquidity_supply AS (

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
LEFT JOIN c_liquidity_supply i ON d.day = i.dt

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

-- days AS (
    
--     SELECT generate_series('2021-02-10'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
-- ),

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
    *
FROM cgi_feed
WHERE usd_price IS NOT NULL

),

cgi AS (

SELECT
    DISTINCT
    t.day,
    'CGI' AS product,
    t.cgi AS total,
    0 AS incentivized,
     t.cgi - 0 AS unincentivized,
    l.reserves AS liquidity
FROM cgi_total_supply t
LEFT JOIN cgi_liquidity_supply l ON t.day = l.dt
WHERE t.day >= '2021-02-11'

),

-- MVI Supply Breakdown
mvi_uniswap_pairs AS (

  SELECT
    '\x72e364f2abdc788b7e918bc238b21f109cd634d7'::bytea AS token0,
    18 as decimals0,
    'MVI' as symbol0,
    '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS token1,
    18 as decimals1,
    'ETH' as symbol1,
    '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff'::bytea AS pair

),


mvi_uniswap_reserves AS (

  SELECT
    AVG(s.reserve0 / 10^p.decimals0) AS reserve0,
    AVG(s.reserve1 / 10^p.decimals1) AS reserve1,
    s.contract_address,
    date_trunc('day', s.evt_block_time) AS dt,
    p.symbol0,
    p.symbol1
  FROM uniswap_v2."Pair_evt_Sync" s
  JOIN mvi_uniswap_pairs p ON s.contract_address = p.pair
  GROUP BY 3, 4, 5, 6

),

mvi_uniswap_supply AS (

    SELECT
        SUM(CASE
            WHEN symbol0 = 'MVI' THEN reserve0
            WHEN symbol1 = 'MVI' THEN reserve1
            ELSE NULL
        END) AS reserves,
        dt,
        'MVI' AS product,
        'uniswap' AS project
    FROM mvi_uniswap_reserves
    GROUP BY 2, 3, 4
 
),

mvi_liquidity_supply_temp AS (

SELECT dt, reserves FROM mvi_uniswap_supply

),

mvi_liquidity_supply AS (

    SELECT
        dt,
        SUM(reserves) AS reserves
    FROM mvi_liquidity_supply_temp
    GROUP BY 1

),

mvi_lp_w_lm_option_supply AS (

    SELECT dt, reserves FROM mvi_uniswap_supply

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

mvi_total_supply AS (

SELECT 
    day, 
    SUM(amount) OVER (ORDER BY day) AS mvi
FROM mvi_units

),

mvi_mint_burn_lp AS (

  SELECT
    tr."from" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'burn' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff'
    AND tr."to" = '\x0000000000000000000000000000000000000000'

  UNION ALL

  SELECT
    tr."to" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'mint' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff'
    AND tr."from" = '\x0000000000000000000000000000000000000000'

),

mvi_mint_burn_lp_temp AS (

SELECT
    evt_block_day,
    SUM(amount) AS lp_amount
FROM mvi_mint_burn_lp
GROUP BY 1
ORDER BY 1

),

mvi_lp AS (

SELECT
    *,
    SUM(lp_amount) OVER (ORDER BY evt_block_day) AS lp_running_amount
FROM mvi_mint_burn_lp_temp

),

mvi_stake_unstake_lm AS (

  SELECT
    tr."from" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'stake' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff'
    AND tr."to" IN ('\x5bC4249641B4bf4E37EF513F3Fa5C63ECAB34881')

  UNION ALL

  SELECT
    tr."to" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'unstake' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff'
    AND tr."from" IN ('\x5bC4249641B4bf4E37EF513F3Fa5C63ECAB34881')

),

mvi_stake_unstake_lm_temp AS (

SELECT
    evt_block_day,
    SUM(amount) AS lm_amount
FROM mvi_stake_unstake_lm
GROUP BY 1
ORDER BY 1

),

mvi_lm AS (

SELECT
    *,
    SUM(lm_amount) OVER (ORDER BY evt_block_day) AS lm_running_amount
FROM mvi_stake_unstake_lm_temp

),

mvi_lp_lm AS (

    SELECT 
        mvi_lp.*,
        COALESCE(mvi_lm.lm_amount, 0) AS lm_amount,
        COALESCE(mvi_lm.lm_running_amount, 0) AS lm_running_amount,
        COALESCE(mvi_lm.lm_running_amount / mvi_lp.lp_running_amount, 0) AS perc_lp_lm
    FROM mvi_lp
    LEFT JOIN mvi_lm USING (evt_block_day)
    
),

mvi_index_rewards AS (

  SELECT
    tr."to" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'reward' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x0954906da0bf32d5479e25f46056d22f08464cab'
    AND tr."from" IN ('\x5bC4249641B4bf4E37EF513F3Fa5C63ECAB34881')

),

mvi_daily_index_rewards AS (

SELECT
    evt_block_day,
    SUM(amount) AS amount
FROM mvi_index_rewards
GROUP BY 1
ORDER BY 1

),

mvi_7day_avg_index_rewards AS (

    SELECT
        evt_block_day,
        AVG(amount) OVER (ORDER BY evt_block_day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS index
    FROM mvi_daily_index_rewards

),

mvi AS (

SELECT
    DISTINCT
    t.day,
    'MVI' AS product,
    t.mvi AS total,
    m.reserves * i.perc_lp_lm AS incentivized,
    t.mvi - (m.reserves * i.perc_lp_lm) AS unincentivized,
    l.reserves AS liquidity
FROM mvi_total_supply t
LEFT JOIN mvi_liquidity_supply l ON t.day = l.dt
LEFT JOIN mvi_lp_w_lm_option_supply m ON t.day = m.dt
LEFT JOIN mvi_lp_lm i ON t.day = i.evt_block_day
LEFT JOIN mvi_7day_avg_index_rewards d ON t.day = d.evt_block_day
WHERE t.day >= '2021-04-06'

),

coop AS (

SELECT * FROM dpi

UNION ALL

SELECT * FROM fli

UNION ALL

SELECT * FROM cgi

UNION ALL

SELECT * FROM mvi

)

SELECT
    day,
    SUM(total) AS total,
    SUM(incentivized) AS incentivized,
    SUM(unincentivized) AS unincentivized,
    SUM(liquidity) AS liquidity
FROM coop
GROUP BY 1
ORDER BY 1
