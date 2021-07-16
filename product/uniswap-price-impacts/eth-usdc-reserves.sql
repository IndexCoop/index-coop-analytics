-- https://duneanalytics.com/queries/64546/128651

WITH v3_supply AS (
    
    WITH  pool as (
    select
                pool,
                token0,
                token1
    from        uniswap_v3."Factory_evt_PoolCreated"
    where       pool = '\x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'
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
                ((sum(amount0) over(order by evt_block_time, a.pool))/10^t0.decimals) * p0.price +
                ((sum(amount1) over(order by evt_block_time, a.pool))/10^t1.decimals) * p1.price as reserves_usd,
                t0.symbol as token0,
                t1.symbol as token1
                
    from        amounts a

    inner join  pool
    on          pool.pool = a.pool

    inner join  tokens t0
    on          t0.contract_address = pool.token0

    inner join  tokens t1
    on          t1.contract_address = pool.token1
    
    left join   prices.usd p0
    on          date_trunc('minute', a.evt_block_time) = p0.minute AND t0.contract_address = p0.contract_address
    
    left join   prices.usd p1
    on          date_trunc('minute', a.evt_block_time) = p1.minute AND t1.contract_address = p1.contract_address
    )

    select
                date_trunc('day', evt_block_time) AS dt,
                AVG(reserves_usd) AS reserves_usd

    from        cumsum_amounts
    group by    1


),

v2_supply AS (

    WITH v2_pool AS (
    
      SELECT
        *,
        6 AS decimals0,
        18 AS decimals1
      FROM uniswap_v2."Factory_evt_PairCreated" pairsraw
      WHERE pair = '\xb4e16d0168e52d35cacd2c6185b44281ec28c9dc'
      
    ),
    
    v2_supply AS (
    
        SELECT
            date_trunc('day', s.evt_block_time) AS dt,
            AVG(s.reserve0 / 10^p.decimals0) AS reserve0,
            AVG(s.reserve1 / 10^p.decimals1) AS reserve1,
            AVG(((s.reserve0 / 10^p.decimals0) * p0.price) + ((s.reserve1 / 10^p.decimals1) * p1.price)) AS reserves_usd
            -- (reserve0 * p0.price) + (reserve1 * p1.price) AS reserves_usd
        FROM uniswap_v2."Pair_evt_Sync" s
        LEFT JOIN v2_pool p ON s.contract_address = p.pair
        INNER JOIN (SELECT * FROM prices.usd WHERE contract_address = '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48') p0 ON date_trunc('minute', s.evt_block_time) = p0.minute
        INNER JOIN (SELECT * FROM prices.usd WHERE contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') p1 ON date_trunc('minute', s.evt_block_time) = p1.minute
        WHERE date_trunc('day', s.evt_block_time) >= '2021-04-01'
        GROUP BY 1
    
    )
    
    SELECT
        dt,
        reserves_usd
    FROM v2_supply

)

SELECT 
*,
'v3' AS version
FROM v3_supply

UNION ALL

SELECT
*,
'v2' AS version
FROM v2_supply