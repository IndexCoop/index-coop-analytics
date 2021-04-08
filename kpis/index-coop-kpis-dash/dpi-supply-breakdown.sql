-- https://duneanalytics.com/queries/27649/56531

-- DPI/ETH LP Staking Contract (INDEX rewards) --> \xB93b505Ed567982E2b6756177ddD23ab5745f309
-- DPI/ETH LP Token / Pool Address --> \x4d5ef58aac27d99935e5b6b4a6778ff292059991
-- DPI --> \x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b
-- DPI/ETH Sushi LP Token / Pool Address --> \x34b13f8cd184f55d0bd4dd1fe6c07d46f245c7ed
-- DPI/ETH Sushi LP Staking Contract --> \xc2EdaD668740f1aA35E4D8f227fB8E17dcA888Cd

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

)

SELECT
    DISTINCT
    t.day,
    'DPI' AS product,
    t.dpi AS total,
    m.reserves * i.perc_lp_lm AS incentivized,
    t.dpi - (m.reserves * i.perc_lp_lm) AS unincentivized,
    l.reserves AS liquidity,
    m.reserves AS liquidity_with_incentive_option,
    i.perc_lp_lm AS liquidity_with_incentive_staked_perc,
    COALESCE(d.index, 0) AS avg_index_rewarded,
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