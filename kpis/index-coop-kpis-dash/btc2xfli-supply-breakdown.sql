--https://duneanalytics.com/queries/77103

-- BTC2x-FLI Supply Breakdown

----------------------------------------------------------
-- Uniswap v3
----------------------------------------------------------
WITH fli_uniswap_v3_supply AS (
    
    WITH  pool as (
    select
                pool,
                token0,
                token1
    from        uniswap_v3."Factory_evt_PoolCreated" 

    where       token0 = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
    or          token1 = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
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
                evt_block_time,
                pool,
                mint0 as amount0,
                mint1 as amount1
    from        mint_agg
    
    union all
    
    select      
                evt_block_time,
                pool,
                (burn0 * -1) as amount0,
                (burn1 * -1) as amount1
    from        burn_agg
    
    union all
    
    select      
                evt_block_time,
                pool,
                swap0 as amount0,
                swap1 as amount1
    from        swap_agg
    )

    , amounts as (
    select
                evt_block_time,
                pool,
                sum(amount0) as amount0,
                sum(amount1) as amount1
    from        mint_burn_swap
    group by    1,2
    )

    -- Final dataset at evt_block_time periodicity and including extra descriptive columns
    , cumsum_amounts as (
    select
                a.*,
                (sum(amount0) over(partition by a.pool order by evt_block_time))/10^t0.decimals as reserve0,
                (sum(amount1) over(partition by a.pool order by evt_block_time))/10^t1.decimals as reserve1,
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
    
    -- Average daily reserves of BTC2X-FLI per pool
    , avg_reserves_pool as (
    select
                date_trunc('day', evt_block_time) as dt,
                pool,
                token0,
                token1,
                avg(reserve0) as reserve0,
                avg(reserve1) as reserve1
                
    from        cumsum_amounts

    group by    1,2,3,4
    )
    
    select
                dt,
                sum(case 
                        when token0 = 'BTC2X-FLI' then reserve0
                        when token1 = 'BTC2X-FLI' then reserve1
                    end) as reserves

    from        avg_reserves_pool

    group by    1
)

----------------------------------------------------------
-- Sushiswap
----------------------------------------------------------
, fli_sushiswap_pairs AS (

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

fli_sushiswap_reserves AS (

  SELECT
    AVG(s.reserve0 / 10^p.decimals0) AS reserve0,
    AVG(s.reserve1 / 10^p.decimals1) AS reserve1,
    s.contract_address,
    date_trunc('day', s.evt_block_time) AS dt,
    p.symbol0,
    p.symbol1
  FROM sushi."Pair_evt_Sync" s
  JOIN fli_sushiswap_pairs p ON s.contract_address = p.pair
  GROUP BY 3, 4, 5, 6

),

fli_sushiswap_supply AS (

    SELECT
        SUM(CASE
            WHEN symbol0 = 'BTC2x-FLI' THEN reserve0
            WHEN symbol1 = 'BTC2x-FLI' THEN reserve1
            ELSE NULL
        END) AS reserves,
        dt,
        'BTC2x-FLI' AS product,
        'sushiswap' AS project
    FROM fli_sushiswap_reserves
    GROUP BY 2, 3, 4
 
),

fli_liquidity_supply_temp AS (

SELECT dt, reserves FROM fli_sushiswap_supply

UNION ALL

SELECT dt, reserves from fli_uniswap_v3_supply

),

fli_liquidity_supply AS (

    SELECT
        dt,
        SUM(reserves) AS reserves
    FROM fli_liquidity_supply_temp
    GROUP BY 1

),
----------------------------------------------------------
-- total fli supply methodology
----------------------------------------------------------
fli_mint_burn AS (
SELECT day, sum(amount) as amount

from
    (
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

    )a
group by 1
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

fli_total_supply AS (

SELECT 
    day, 
    SUM(amount) OVER (ORDER BY day) AS fli
FROM fli_units

),

-----------------------------------------------------------
-- fli price feed
----------------------------------------------------------

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
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
    (ARRAY_REMOVE(ARRAY_AGG(btc_price) OVER (ORDER BY hour), NULL))[COUNT(btc_price) OVER (ORDER BY hour)] AS btc_price
FROM fli_temp

),

fli_price_feed AS (

SELECT
    date_trunc('day', hour) AS dt,
    AVG(usd_price) AS price
FROM fli_feed
WHERE usd_price IS NOT NULL
GROUP BY 1

)


SELECT
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
FROM fli_total_supply t
LEFT JOIN fli_liquidity_supply l ON t.day = l.dt
LEFT JOIN fli_price_feed p on t.day = p.dt
WHERE t.day >= '2021-05-11'
