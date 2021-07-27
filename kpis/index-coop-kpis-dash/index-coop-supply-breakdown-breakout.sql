-- https://duneanalytics.com/queries/77496

-- Index Coop Supply Breakdown

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
    d.day AS evt_block_day,
    COALESCE(SUM(p.amount), 0) AS lp_amount
FROM dpi_days d
LEFT JOIN dpi_mint_burn_lp p ON d.day = p.evt_block_day
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
    d.day AS evt_block_day,
    COALESCE(SUM(s.amount), 0) AS lm_amount
FROM dpi_days d
LEFT JOIN dpi_stake_unstake_lm s ON d.day = s.evt_block_day
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
        dpi_lp.*,
        COALESCE(dpi_lm.lm_amount, 0) AS lm_amount,
        COALESCE(dpi_lm.lm_running_amount, 0) AS lm_running_amount,
        COALESCE(dpi_lm.lm_running_amount / dpi_lp.lp_running_amount, 0) AS perc_lp_lm
    FROM dpi_lp
    LEFT JOIN dpi_lm USING (evt_block_day)
    
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

dpi_daily_price_feed AS (

WITH prices_usd AS (

    SELECT
        date_trunc('day', minute) AS dt,
        AVG(price) AS price
    FROM prices.usd
    WHERE symbol = 'DPI'
    GROUP BY 1
    ORDER BY 1
    
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
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
    (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
FROM dpi_temp

),

dpi_price_feed AS (

    SELECT
        date_trunc('day', hour) AS dt,
        AVG(usd_price) AS price
    FROM dpi_feed
    WHERE date_trunc('day', hour) NOT IN (SELECT dt FROM prices_usd)
        AND usd_price IS NOT NULL
    GROUP BY 1

),

dpi_price AS (

SELECT
    *
FROM prices_usd

UNION ALL

SELECT
    *
FROM dpi_price_feed

)

SELECT
    *
FROM dpi_price
WHERE dt > '2020-09-10'
ORDER BY 1

),

dpi AS (

SELECT
    DISTINCT
    t.day,
    'DPI' AS product,
    t.dpi AS total,
    m.reserves * i.perc_lp_lm AS incentivized,
    t.dpi - (m.reserves * i.perc_lp_lm) AS unincentivized,
    l.reserves AS liquidity,
    t.dpi * p.price AS tvl,
    m.reserves * i.perc_lp_lm * p.price AS itvl,
    (t.dpi - (m.reserves * i.perc_lp_lm)) * p.price AS utvl,
    l.reserves * price AS liquidity_value,
    p.price
FROM dpi_total_supply t
LEFT JOIN dpi_liquidity_supply l ON t.day = l.dt
LEFT JOIN dpi_lp_w_lm_option_supply m ON t.day = m.dt
LEFT JOIN dpi_lp_lm i ON t.day = i.evt_block_day
LEFT JOIN dpi_7day_avg_index_rewards d ON t.day = d.evt_block_day
LEFT JOIN dpi_daily_price_feed AS p ON t.day = p.dt
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
        AND sw.evt_block_time >= '202-02-11'

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
    date_trunc('day', hour) AS dt,
    AVG(usd_price) AS price
FROM fli_feed
WHERE usd_price IS NOT NULL
GROUP BY 1

),

fli AS (

SELECT
    DISTINCT
    t.day,
    'ETH2x-FLI' AS product,
    t.fli AS total,
    0 AS incentivized,
    t.fli - 0 AS unincentivized,
    l.reserves AS liquidity,
    t.fli * p.price AS tvl,
    0 * p.price AS itvl,
    (t.fli - 0) * p.price AS utvl,
    l.reserves * p.price AS liquidity_value,
    p.price
FROM fli_total_supply t
LEFT JOIN fli_liquidity_supply l ON t.day = l.dt
LEFT JOIN fli_price_feed p on t.day = p.dt
WHERE t.day >= '2021-03-14'

),

-- BTC2x-FLI Supply Breakdown
btc2x_sushiswap_pairs AS (

  SELECT
    token0,
    18 as decimals0,
    'BTC2x-FLI' as symbol0,
    token1,
    18 as decimals1,
    'wBTC' as symbol1,
    pair
  FROM sushi."Factory_evt_PairCreated" pairsraw
  WHERE pair = '\x164fe0239d703379bddde3c80e4d4800a1cd452b'
  
),

btc2x_sushiswap_reserves AS (

  SELECT
    AVG(s.reserve0 / 10^p.decimals0) AS reserve0,
    AVG(s.reserve1 / 10^p.decimals1) AS reserve1,
    s.contract_address,
    date_trunc('day', s.evt_block_time) AS dt,
    p.symbol0,
    p.symbol1
  FROM sushi."Pair_evt_Sync" s
  JOIN btc2x_sushiswap_pairs p ON s.contract_address = p.pair
  GROUP BY 3, 4, 5, 6

),

btc2x_sushiswap_supply AS (

    SELECT
        SUM(CASE
            WHEN symbol0 = 'BTC2x-FLI' THEN reserve0
            WHEN symbol1 = 'BTC2x-FLI' THEN reserve1
            ELSE NULL
        END) AS reserves,
        dt,
        'BTC2x-FLI' AS product,
        'sushiswap' AS project
    FROM btc2x_sushiswap_reserves
    GROUP BY 2, 3, 4
 
),

btc2x_liquidity_supply_temp AS (

SELECT dt, reserves FROM btc2x_sushiswap_supply

),

btc2x_liquidity_supply AS (

    SELECT
        dt,
        SUM(reserves) AS reserves
    FROM btc2x_liquidity_supply_temp
    GROUP BY 1

),

btc2x_mint_burn AS (

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

btc2x_days AS (
    
    SELECT generate_series('2021-05-11'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

btc2x_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM btc2x_days d
    LEFT JOIN btc2x_mint_burn m ON d.day = m.day
    
),

btc2x_total_supply AS (

SELECT 
    day, 
    SUM(amount) OVER (ORDER BY day) AS fli
FROM btc2x_units

),

--btc2x price feed
btc2x_swap AS (

-- btc2x/wbtc sushi x164fe0239d703379bddde3c80e4d4800a1cd452b
    
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour,
        ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
        ("amount1In" + "amount1Out")/1e8 AS a1_amt
    FROM sushi."Pair_evt_Swap" sw
    WHERE contract_address = '\x164fe0239d703379bddde3c80e4d4800a1cd452b' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2021-05-11'

),

btc2x_a1_prcs AS (

    SELECT 
        avg(price) a1_prc, 
        date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2021-05-11'
        AND contract_address ='\x2260fac5e5542a773aa44fbcfedf7c193bc2c599' --wbtc as base asset
    GROUP BY 2
                
),

btc2x_hours AS (
    
    SELECT generate_series('2021-05-11 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
    
),

btc2x_temp AS (

SELECT
    h.hour,
    COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price, 
    COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as btc_price
    -- a1_prcs."minute" AS minute
FROM btc2x_hours h
LEFT JOIN btc2x_swap s ON s."hour" = h.hour 
LEFT JOIN btc2x_a1_prcs a ON h."hour" = a."hour"
GROUP BY 1

),

btc2x_feed AS (

SELECT
    hour,
    'BTC2x-FLI' AS product,
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
    (ARRAY_REMOVE(ARRAY_AGG(btc_price) OVER (ORDER BY hour), NULL))[COUNT(btc_price) OVER (ORDER BY hour)] AS btc_price
FROM btc2x_temp

),

btc2x_price_feed AS (

SELECT
    date_trunc('day', hour) AS dt,
    AVG(usd_price) AS price
FROM btc2x_feed
WHERE usd_price IS NOT NULL
GROUP BY 1

),

btc2x AS (

    SELECT
        DISTINCT
        t.day,
        'BTC2x-FLI' AS product,
        t.fli AS total,
        0 AS incentivized,
        t.fli - 0 AS unincentivized,
        l.reserves AS liquidity,
        t.fli * p.price AS tvl,
        0 * p.price AS itvl,
        (t.fli - 0) * p.price AS utvl,
        l.reserves * p.price AS liquidity_value,
        p.price
    FROM btc2x_total_supply t
    LEFT JOIN btc2x_liquidity_supply l ON t.day = l.dt
    LEFT JOIN btc2x_price_feed p on t.day = p.dt
    WHERE t.day >= '2021-05-11'

),

-- MVI Supply Breakdown
mvi_uniswap_pairs AS (

  SELECT
    '\x72e364f2abdc788b7e918bc238b21f109cd634d7'::bytea AS token0,
    18 as decimals0,
    'MVI' as symbol0,
    '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS token1,
    18 as decimals1,
    'wETH' as symbol1,
    '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff'::bytea AS pair
--   FROM uniswap_v2."Factory_evt_PairCreated" pairsraw
--   LEFT JOIN erc20.tokens erc20 ON pairsraw.token0 = erc20.contract_address
--   LEFT JOIN erc20.tokens erc202 ON pairsraw.token1 = erc202.contract_address
-- --   WHERE token0 IN (SELECT DISTINCT contract_address FROM erc20.tokens WHERE decimals > 0)
-- --     AND token1 IN (SELECT DISTINCT contract_address FROM erc20.tokens WHERE decimals > 0)
--   WHERE erc20.symbol = 'MVI' OR
--     erc202.symbol = 'MVI'

),

-- WITH uniswap_pairs AS (

--   SELECT
--     token0,
--     erc20.decimals as decimals0,
--     erc20.symbol as symbol0,
--     token1,
--     erc202.decimals as decimals1,
--     erc202.symbol as symbol1,
--     pair
--   FROM uniswap_v2."Factory_evt_PairCreated" pairsraw
--   LEFT JOIN erc20.tokens erc20 ON pairsraw.token0 = erc20.contract_address
--   LEFT JOIN erc20.tokens erc202 ON pairsraw.token1 = erc202.contract_address
-- --   WHERE token0 IN (SELECT DISTINCT contract_address FROM erc20.tokens WHERE decimals > 0)
-- --     AND token1 IN (SELECT DISTINCT contract_address FROM erc20.tokens WHERE decimals > 0)
--   WHERE erc20.symbol = 'MVI' OR
--     erc202.symbol = 'MVI'
  
-- ),

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
    d.day AS evt_block_day,
    COALESCE(SUM(p.amount), 0) AS lp_amount
FROM mvi_days d
LEFT JOIN mvi_mint_burn_lp p ON d.day = p.evt_block_day
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
    d.day AS evt_block_day,
    COALESCE(SUM(s.amount), 0) AS lm_amount
FROM mvi_days d
LEFT JOIN mvi_stake_unstake_lm s ON d.day = s.evt_block_day
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

mvi_swap AS (

--eth/mvi uni        x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff
    
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour,
        ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
        ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM uniswap_v2."Pair_evt_Swap" sw
    WHERE contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2021-04-06'

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
    
    SELECT generate_series('2021-04-06 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
    
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
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
    (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
FROM mvi_temp

),

mvi_price_feed AS (

SELECT
    date_trunc('day', hour) AS dt,
    AVG(usd_price) AS price
FROM mvi_feed
WHERE usd_price IS NOT NULL
GROUP BY 1

),

mvi AS (

SELECT
    DISTINCT
    t.day,
    'MVI' AS product,
    t.mvi AS total,
    m.reserves * i.perc_lp_lm AS incentivized,
    t.mvi - (m.reserves * i.perc_lp_lm) AS unincentivized,
    l.reserves AS liquidity,
    t.mvi * p.price AS tvl,
    m.reserves * i.perc_lp_lm * p.price AS itvl,
    (t.mvi - (m.reserves * i.perc_lp_lm)) * p.price AS utvl,
    l.reserves * p.price AS liquidity_value,
    p.price
FROM mvi_total_supply t
LEFT JOIN mvi_liquidity_supply l ON t.day = l.dt
LEFT JOIN mvi_lp_w_lm_option_supply m ON t.day = m.dt
LEFT JOIN mvi_lp_lm i ON t.day = i.evt_block_day
LEFT JOIN mvi_7day_avg_index_rewards d ON t.day = d.evt_block_day
LEFT JOIN mvi_price_feed p ON t.day = p.dt
WHERE t.day > '2021-04-06'

),

-- bed supply breakdown
bed_uniswap_v3_supply AS (
    
    WITH  pool as (
    select
                pool,
                token0,
                token1
    from        uniswap_v3."Factory_evt_PoolCreated"
    where       pool = '\x779DFffB81550BF503C19d52b1e91e9251234fAA'
    )

    , tokens as (
    
    select      * 
    from        erc20."tokens"
        
    )

    -- Liquidity added to the pool
    , mint as (
    select      *
    from        uniswap_v3."Pair_evt_Mint" a
    inner join  pool
    on          pool.pool = a.contract_address
    )

    -- Liquidity removed from the pool
    , burn as (
    select      *
    from        uniswap_v3."Pair_evt_Burn" a
    inner join  pool
    on          pool.pool = a.contract_address
    )

    -- Swaps
    , swap as (
    select      * 
    from        uniswap_v3."Pair_evt_Swap" a
    inner join  pool
    on          pool.pool = a.contract_address
    )

    -- Aggregating data to evt_block_time level so duplicates due to activity at the same evt_block_time are avoided
    , mint_agg as (
    select
                evt_block_time,
                pool,
                sum(amount0) as mint0,
                sum(amount1) as mint1
    from        mint
    group by    1,2
    )

    , burn_agg as (
    select
                evt_block_time,
                pool,
                sum(amount0) as burn0,
                sum(amount1) as burn1
    from        burn
    group by    1,2
    )

    , swap_agg as (
    select      
                evt_block_time,
                pool,
                sum(amount0) as swap0,
                sum(amount1) as swap1

    from        swap
    group by    1,2
    )

    , mint_burn_swap as (
    select      
                coalesce(a.evt_block_time, b.evt_block_time, c.evt_block_time) as evt_block_time,
                coalesce(a.pool, b.pool, c.pool) as pool,
                mint0,
                mint1,
                (burn0 * -1) as burn0,
                (burn1 * -1) as burn1,
                swap0,
                swap1

    from        mint_agg a
    full outer join burn_agg b
    on          a.evt_block_time = b.evt_block_time
    and         a.pool = b.pool
    full outer join swap_agg c
    on          a.evt_block_time = c.evt_block_time
    and         a.pool = c.pool
    )

    , amounts as (
    select
                evt_block_time,
                pool,
                coalesce(mint0,0) + coalesce(burn0,0) + coalesce(swap0,0) as amount0,
                coalesce(mint1,0) + coalesce(burn1,0) + coalesce(swap1,0) as amount1,
                mint0,
                mint1,
                burn0,
                burn1,
                swap0,
                swap1
    from        mint_burn_swap
    )

    -- Final dataset at evt_block_time periodicity and including extra descriptive columns
    , cumsum_amounts as (
    select
                a.*,
                (sum(amount0) over(order by evt_block_time, a.pool))/10^t0.decimals as reserve0,
                (sum(amount1) over(order by evt_block_time, a.pool))/10^t1.decimals as reserve1,
                t0.symbol as token0,
                t1.symbol as token1
                
    from        amounts a

    inner join  pool
    on          pool.pool = a.pool

    inner join  tokens t0
    on          t0.contract_address = pool.token0

    inner join  tokens t1
    on          t1.contract_address = pool.token1
    )

    -- Average daily reserves of ETH2X-FLI on Uniswap v3
    select
                date_trunc('day', evt_block_time) as dt,
                avg(reserve0) as reserves

    from        cumsum_amounts

    group by    1


),

bed_liquidity_supply_temp AS (

SELECT dt, reserves from bed_uniswap_v3_supply

),

bed_liquidity_supply AS (

    SELECT
        dt,
        SUM(reserves) AS reserves
    FROM bed_liquidity_supply_temp
    GROUP BY 1

),

bed_mint_burn AS (

SELECT day, sum(amount) as amount

from
    (
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
    
    )a
group by 1
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

bed_total_supply AS (

SELECT 
    day, 
    SUM(amount) OVER (ORDER BY day) AS bed
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

bed_price_feed AS (

SELECT
    date_trunc('day', hour) AS dt,
    AVG(usd_price) AS price
FROM bed_feed
WHERE usd_price IS NOT NULL
GROUP BY 1

),

bed AS (

SELECT
    DISTINCT
    t.day,
    'BED' AS product,
    t.bed AS total,
    0 AS incentivized,
    t.bed - 0 AS unincentivized,
    l.reserves AS liquidity,
    t.bed * p.price AS tvl,
    0 * p.price AS itvl,
    (t.bed - 0) * p.price AS utvl,
    l.reserves * p.price AS liquidity_value,
    p.price
FROM bed_total_supply t
LEFT JOIN bed_liquidity_supply l ON t.day = l.dt
LEFT JOIN bed_price_feed p on t.day = p.dt
WHERE t.day >= '2021-07-21'

),

coop AS (

SELECT * FROM dpi

UNION ALL

SELECT * FROM fli

UNION ALL

SELECT * FROM btc2x

UNION ALL

SELECT * FROM mvi

UNION ALL

SELECT * FROM bed

),

total AS (

    SELECT
        day,
        'Total' AS product,
        SUM(tvl) AS tvl,
        SUM(itvl) AS itvl,
        SUM(utvl) AS utvl
    FROM coop
    GROUP BY 1, 2

)

SELECT
    day,
    product,
    tvl,
    itvl,
    utvl
FROM coop

UNION ALL

SELECT * FROM total

