-- https://duneanalytics.com/queries/30223

-- MVI Details
-- Token: 0x72e364f2abdc788b7e918bc238b21f109cd634d7
-- Staking: 0x5bC4249641B4bf4E37EF513F3Fa5C63ECAB34881
-- Uni LP / Pool: 0x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff

WITH mvi_uniswap_pairs AS (

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
  WHERE erc20.symbol = 'MVI' OR
    erc202.symbol = 'MVI'

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

mvi_uniswap_v3_supply AS (
    
    WITH  pool as (
    select
                pool,
                token0,
                token1
    from        uniswap_v3."Factory_evt_PoolCreated"
    where       pool = '\x06aDA8f74D99C6C200672b02E5C3341866cA3bFB'
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
                avg(reserve0) as reserves,
                date_trunc('day', evt_block_time) as dt,
                'MVI' AS product,
                'uniswap v3' AS project

    from        cumsum_amounts

    group by    2, 3, 4


),

mvi_liquidity_supply_temp AS (

SELECT dt, reserves FROM mvi_uniswap_supply

UNION ALL

SELECT dt, reserves FROM mvi_uniswap_v3_supply

),

mvi_liquidity_supply AS (

    SELECT
        dt,
        SUM(reserves) AS reserves
    FROM mvi_liquidity_supply_temp
    GROUP BY 1

),

mvi_lp_w_lm_option_supply AS (

    SELECT 
        dt, 
        CASE 
            WHEN dt <= '09-19-2021' THEN reserves 
            ELSE 0
        END AS reserves
    FROM mvi_uniswap_supply

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

)

SELECT
    DISTINCT
    t.day,
    'MVI' AS product,
    t.mvi AS total,
    m.reserves * i.perc_lp_lm AS incentivized,
    t.mvi - (m.reserves * i.perc_lp_lm) AS unincentivized,
    l.reserves AS liquidity,
    m.reserves AS liquidity_with_incentive_option,
    i.perc_lp_lm AS liquidity_with_incentive_staked_perc,
    COALESCE(d.index, 0) AS avg_index_rewarded,
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