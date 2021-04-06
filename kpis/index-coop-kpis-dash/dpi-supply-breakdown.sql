-- https://duneanalytics.com/queries/27649/56531

-- DPI/ETH LP Staking Contract (INDEX rewards) --> \xB93b505Ed567982E2b6756177ddD23ab5745f309
-- DPI/ETH LP Token / Pool Address --> \x4d5ef58aac27d99935e5b6b4a6778ff292059991
-- DPI --> \x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b
-- DPI/ETH Sushi LP Token / Pool Address --> \x34b13f8cd184f55d0bd4dd1fe6c07d46f245c7ed
-- DPI/ETH Sushi LP Staking Contract --> \xc2EdaD668740f1aA35E4D8f227fB8E17dcA888Cd

  WITH uniswap_pairs AS (

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

uniswap_reserves AS (

  SELECT
    AVG(s.reserve0 / 10^p.decimals0) AS reserve0,
    AVG(s.reserve1 / 10^p.decimals1) AS reserve1,
    s.contract_address,
    date_trunc('day', s.evt_block_time) AS dt,
    p.symbol0,
    p.symbol1
  FROM uniswap_v2."Pair_evt_Sync" s
  JOIN uniswap_pairs p ON s.contract_address = p.pair
  GROUP BY 3, 4, 5, 6

),

uniswap_supply AS (

    SELECT
        SUM(CASE
            WHEN symbol0 = 'DPI' THEN reserve0
            WHEN symbol1 = 'DPI' THEN reserve1
            ELSE NULL
        END) AS reserves,
        dt,
        'DPI' AS product,
        'uniswap' AS project
    FROM uniswap_reserves
    GROUP BY 2, 3, 4
 
),

sushi_pairs AS (

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

sushi_reserves AS (

  SELECT
    AVG(s.reserve0 / 10^p.decimals0) AS reserve0,
    AVG(s.reserve1 / 10^p.decimals1) AS reserve1,
    s.contract_address,
    date_trunc('day', s.evt_block_time) AS dt,
    p.symbol0,
    p.symbol1
  FROM sushi."Pair_evt_Sync" s
  JOIN sushi_pairs p ON s.contract_address = p.pair
  GROUP BY 3, 4, 5, 6

),

sushi_supply AS (

    SELECT
        SUM(CASE
            WHEN symbol0 = 'DPI' THEN reserve0
            WHEN symbol1 = 'DPI' THEN reserve1
            ELSE NULL
        END) AS reserves,
        dt,
        'DPI' AS product,
        'sushiswap' AS project
    FROM sushi_reserves
    GROUP BY 2, 3, 4
 
),

balancer_supply AS (

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

liquidity_supply_temp AS (

SELECT dt, reserves FROM uniswap_supply

UNION ALL

SELECT dt, reserves FROM sushi_supply

UNION ALL

SELECT dt, reserves FROM balancer_supply

),

liquidity_supply AS (

    SELECT
        dt,
        SUM(reserves) AS reserves
    FROM liquidity_supply_temp
    GROUP BY 1

),

lp_w_lm_option_supply AS (

    SELECT dt, reserves FROM uniswap_supply

),

mint_burn AS (

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

days AS (
    
    SELECT generate_series('2020-09-10'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM days d
    LEFT JOIN mint_burn m ON d.day = m.day
    
),

total_supply AS (

SELECT 
    day, 
    SUM(amount) OVER (ORDER BY day) AS dpi
FROM units

),

mint_burn_lp AS (

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

mint_burn_lp_temp AS (

SELECT
    evt_block_day,
    SUM(amount) AS lp_amount
FROM mint_burn_lp
GROUP BY 1
ORDER BY 1

),

lp AS (

SELECT
    *,
    SUM(lp_amount) OVER (ORDER BY evt_block_day) AS lp_running_amount
FROM mint_burn_lp_temp

),

stake_unstake_lm AS (

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

stake_unstake_lm_temp AS (

SELECT
    evt_block_day,
    SUM(amount) AS lm_amount
FROM stake_unstake_lm
GROUP BY 1
ORDER BY 1

),

lm AS (

SELECT
    *,
    SUM(lm_amount) OVER (ORDER BY evt_block_day) AS lm_running_amount
FROM stake_unstake_lm_temp

),

lp_lm AS (

    SELECT 
        lp.*,
        COALESCE(lm.lm_amount, 0) AS lm_amount,
        COALESCE(lm.lm_running_amount, 0) AS lm_running_amount,
        COALESCE(lm.lm_running_amount / lp.lp_running_amount, 0) AS perc_lp_lm
    FROM lp
    LEFT JOIN lm USING (evt_block_day)
    
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
    COALESCE(d.index, 0) AS avg_index_rewarded
    -- t.dpi - l.reserves AS passive,
    -- t.dpi * p.price AS TVL,
    -- l.reserves * p.price AS liquidityTVL,
    -- (t.dpi - l.reserves) * p.price AS passiveTVL,
    -- (t.dpi - l.reserves) / t.dpi AS uperc
FROM total_supply t
LEFT JOIN liquidity_supply l ON t.day = l.dt
LEFT JOIN lp_w_lm_option_supply m ON t.day = m.dt
LEFT JOIN lp_lm i ON t.day = i.evt_block_day
LEFT JOIN dpi_7day_avg_index_rewards d ON t.day = d.evt_block_day
WHERE t.day >= '2020-10-06'
-- JOIN prices.usd p ON 'DPI' = p.symbol AND p.minute = t.day
