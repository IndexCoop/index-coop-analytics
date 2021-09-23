-- https://dune.xyz/queries/163260

-- DATA Supply Breakdown
WITH token as (

    -- SELECT
    --     * 
    -- FROM erc20.tokens 
    -- WHERE symbol = 'DATA'
    
    SELECT
        '\x33d63ba1e57e54779f7ddaeaa7109349344cf5f1'::bytea AS contract_address,
        18 AS decimals,
        'DATA' AS symbol
    
),

----------------------------------------------------------
-- sushi supply
----------------------------------------------------------
sushi_v2_pools AS (

    SELECT
        token0,
        erc20.decimals as decimals0,
        erc20.symbol as symbol0,
        token1,
        y.decimals as decimals1,
        y.symbol as symbol1,
        pair as pool
    FROM sushi."Factory_evt_PairCreated" pairsraw
    INNER JOIN token erc20 ON pairsraw.token0 = erc20.contract_address
    INNER JOIN erc20.tokens y ON pairsraw.token1 = y.contract_address
    
    UNION ALL
    
    SELECT
        token0,
        x.decimals as decimals0,
        x.symbol as symbol0,
        token1,
        erc202.decimals as decimals1,
        erc202.symbol as symbol1,
        pair as pool
    FROM sushi."Factory_evt_PairCreated" pairsraw
    INNER JOIN token erc202 ON pairsraw.token1 = erc202.contract_address
    INNER JOIN erc20.tokens x ON pairsraw.token0 = x.contract_address

),
    
data_sushi_reserves AS (

  SELECT
    AVG(s.reserve0 / 10^p.decimals0) AS reserve0,
    AVG(s.reserve1 / 10^p.decimals1) AS reserve1,
    s.contract_address,
    date_trunc('day', s.evt_block_time) AS dt,
    p.symbol0,
    p.symbol1
  FROM sushi."Pair_evt_Sync" s
  JOIN sushi_v2_pools p ON s.contract_address = p.pool
  GROUP BY 3, 4, 5, 6

),

data_sushi_supply AS (

    SELECT
        SUM(CASE
            WHEN symbol0 = 'DATA' THEN reserve0
            WHEN symbol1 = 'DATA' THEN reserve1
            ELSE NULL
        END) AS reserves,
        dt,
        'DATA' AS product,
        'sushiswap' AS project
    FROM data_sushi_reserves
    GROUP BY 2, 3, 4
 
),

data_liquidity_supply_temp AS (

    SELECT dt, reserves from data_sushi_supply

),

data_liquidity_supply AS (

    SELECT
        dt,
        SUM(reserves) AS reserves
    FROM data_liquidity_supply_temp
    GROUP BY 1

),

----------------------------------------------------------
-- total data supply methodology
----------------------------------------------------------
data_mint_burn AS (

    SELECT 
        day,    
        sum(amount) as amount
    FROM (
    
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
    
    ) a 
    GROUP BY 1

),

data_days AS (
    
    SELECT generate_series('2021-09-20'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

data_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM data_days d
    LEFT JOIN data_mint_burn m ON d.day = m.day
    
),

data_total_supply AS (

SELECT 
    day, 
    SUM(amount) OVER (ORDER BY day) AS data
FROM data_units

),

----------------------------------------------------------
-- data price feed
----------------------------------------------------------
data_swap AS (

--eth/data sushi 0x208226200b45b82212b814f49efa643980a7bdd1
    
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour,
        ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
        ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM sushi."Pair_evt_Swap" sw
    WHERE contract_address = '\x208226200b45b82212b814f49efa643980a7bdd1' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2021-09-20'

),

data_a1_prcs AS (

    SELECT 
        avg(price) a1_prc, 
        date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2021-09-20'
        AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --weth as base asset
    GROUP BY 2
                
),

data_hours AS (
    
    SELECT generate_series('2021-09-20 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
    
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

data_price_feed AS (

    SELECT
        date_trunc('day', hour) AS dt,
        AVG(usd_price) AS price
    FROM data_feed
    WHERE usd_price IS NOT NULL
    GROUP BY 1

)

SELECT
    t.day,
    'DATA' AS product,
    t.data AS total,
    l.reserves AS incentivized,
    t.data - l.reserves AS unincentivized,
    l.reserves AS liquidity,
    t.data * p.price AS tvl,
    0 * p.price AS itvl,
    (t.data - 0) * p.price AS utvl,
    l.reserves * p.price AS liquidity_value,
    p.price
FROM data_total_supply t
LEFT JOIN data_liquidity_supply l ON t.day = l.dt
LEFT JOIN data_price_feed p on t.day = p.dt
WHERE t.day >= '2021-09-21'
