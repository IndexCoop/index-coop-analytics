-- https://duneanalytics.com/queries/27981/56548

-- ETH2x-FLI Supply Breakdown
WITH uniswap_pairs AS (

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
            WHEN symbol0 = 'FLI' THEN reserve0
            WHEN symbol1 = 'FLI' THEN reserve1
            ELSE NULL
        END) AS reserves,
        dt,
        'FLI' AS product,
        'uniswap' AS project
    FROM uniswap_reserves
    GROUP BY 2, 3, 4
 
),

liquidity_supply_temp AS (

SELECT dt, reserves FROM uniswap_supply

),

liquidity_supply AS (

    SELECT
        dt,
        SUM(reserves) AS reserves
    FROM liquidity_supply_temp
    GROUP BY 1

),

mint_burn AS (

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

days AS (
    
    SELECT generate_series('2021-03-13'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
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
    SUM(amount) OVER (ORDER BY day) AS fli
FROM units

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
    *
FROM fli_feed
WHERE usd_price IS NOT NULL

)

-- mint_burn_lp AS (

--   SELECT
--     tr."from" AS address,
--     -tr.value / 1e18 AS amount,
--     date_trunc('day', evt_block_time) AS evt_block_day,
--     'burn' AS type,
--     evt_tx_hash
--   FROM erc20."ERC20_evt_Transfer" tr
--   WHERE contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
--     AND tr."to" = '\x0000000000000000000000000000000000000000'

--   UNION ALL

--   SELECT
--     tr."to" AS address,
--     tr.value / 1e18 AS amount,
--     date_trunc('day', evt_block_time) AS evt_block_day,
--     'mint' AS type,
--     evt_tx_hash
--   FROM erc20."ERC20_evt_Transfer" tr
--   WHERE contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
--     AND tr."from" = '\x0000000000000000000000000000000000000000'

-- ),

-- mint_burn_lp_temp AS (

-- SELECT
--     evt_block_day,
--     SUM(amount) AS lp_amount
-- FROM mint_burn_lp
-- GROUP BY 1
-- ORDER BY 1

-- ),

-- lp AS (

-- SELECT
--     *,
--     SUM(lp_amount) OVER (ORDER BY evt_block_day) AS lp_running_amount
-- FROM mint_burn_lp_temp

-- ),

-- stake_unstake_lm AS (

--   SELECT
--     tr."from" AS address,
--     tr.value / 1e18 AS amount,
--     date_trunc('day', evt_block_time) AS evt_block_day,
--     'stake' AS type,
--     evt_tx_hash
--   FROM erc20."ERC20_evt_Transfer" tr
--   WHERE contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
--     AND tr."to" IN ('\x8f06FBA4684B5E0988F215a47775Bb611Af0F986', '\xB93b505Ed567982E2b6756177ddD23ab5745f309')

--   UNION ALL

--   SELECT
--     tr."to" AS address,
--     -tr.value / 1e18 AS amount,
--     date_trunc('day', evt_block_time) AS evt_block_day,
--     'unstake' AS type,
--     evt_tx_hash
--   FROM erc20."ERC20_evt_Transfer" tr
--   WHERE contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
--     AND tr."from" IN ('\x8f06FBA4684B5E0988F215a47775Bb611Af0F986', '\xB93b505Ed567982E2b6756177ddD23ab5745f309')

-- ),

-- stake_unstake_lm_temp AS (

-- SELECT
--     evt_block_day,
--     SUM(amount) AS lm_amount
-- FROM stake_unstake_lm
-- GROUP BY 1
-- ORDER BY 1

-- ),

-- lm AS (

-- SELECT
--     *,
--     SUM(lm_amount) OVER (ORDER BY evt_block_day) AS lm_running_amount
-- FROM stake_unstake_lm_temp

-- ),

-- lp_lm AS (

--     SELECT 
--         lp.*,
--         COALESCE(lm.lm_amount, 0) AS lm_amount,
--         COALESCE(lm.lm_running_amount, 0) AS lm_running_amount,
--         COALESCE(lm.lm_running_amount / lp.lp_running_amount, 0) AS perc_lp_lm
--     FROM lp
--     LEFT JOIN lm USING (evt_block_day)
    
-- ),

-- dpi_index_rewards AS (

--   SELECT
--     tr."to" AS address,
--     tr.value / 1e18 AS amount,
--     date_trunc('day', evt_block_time) AS evt_block_day,
--     'reward' AS type,
--     evt_tx_hash
--   FROM erc20."ERC20_evt_Transfer" tr
--   WHERE contract_address = '\x0954906da0bf32d5479e25f46056d22f08464cab'
--     AND tr."from" IN ('\x8f06FBA4684B5E0988F215a47775Bb611Af0F986', '\xB93b505Ed567982E2b6756177ddD23ab5745f309')

-- ),

-- dpi_daily_index_rewards AS (

-- SELECT
--     evt_block_day,
--     SUM(amount) AS amount
-- FROM dpi_index_rewards
-- GROUP BY 1
-- ORDER BY 1

-- ),

-- dpi_7day_avg_index_rewards AS (

--     SELECT
--         evt_block_day,
--         AVG(amount) OVER (ORDER BY evt_block_day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS index
--     FROM dpi_daily_index_rewards
-- )

SELECT
    DISTINCT
    t.day,
    'ETH2X-FLI' AS product,
    t.fli AS total,
    l.reserves AS liquidity,
    t.fli - 0 AS unincentivized,
    0 AS incentivized
    -- l.reserves AS liquidity,
    -- m.reserves AS liquidity_with_incentive_option,
    -- i.perc_lp_lm AS liquidity_with_incentive_staked_perc,
    -- COALESCE(d.index, 0) AS avg_index_rewarded
    -- t.fli * p.usd_price AS tvl,
    -- i.reserves * p.usd_price AS itvl,
    -- (t.fli - i.reserves) * p.usd_price AS utvl,
    -- (t.fli - i.reserves) / t.fli AS uperc
FROM total_supply t
LEFT JOIN liquidity_supply l ON t.day = l.dt
WHERE t.day >= '2021-03-14'
-- LEFT JOIN lp_w_lm_option_supply m ON t.day = m.dt
-- LEFT JOIN lp_lm i ON t.day = i.evt_block_day
-- LEFT JOIN dpi_7day_avg_index_rewards d ON t.day = d.evt_block_day
-- JOIN fli_price_feed p ON t.day = p.hour
