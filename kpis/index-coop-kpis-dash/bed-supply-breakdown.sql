-- https://duneanalytics.com/queries/89099

-- BED Supply Breakdown

----------------------------------------------------------
-- Uniswap v3
----------------------------------------------------------
WITH bed_uniswap_v3_supply AS (
    
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

----------------------------------------------------------
-- total bed supply methodology
----------------------------------------------------------
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

----------------------------------------------------------
-- bed price feed
----------------------------------------------------------

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

)

SELECT
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
