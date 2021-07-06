-- https://duneanalytics.com/queries/75899

WITH pool as (
select 
    '\xcbcdf9626bc03e24f779434178a73a0b4bad62ed'::bytea as contract_address
)

, mb AS (     --GET ALL MINT AND BURN SUCCESSEFUL CALLS (could also use evt table)
                SELECT                          --get all mint calls
                    (date_trunc('seconds', (call_block_time - timestamptz 'epoch') / 14400) * 14400 + timestamptz 'epoch') as tp, -- periodicity of final result
                    "tickLower" AS lowerTick,   --range lower limit
                    "tickUpper" AS UpperTick,   --range upper limit
                    amount                      --Liquidity added to each tick
                FROM uniswap_v3."Pair_call_mint"
                WHERE call_success = true       --exclude fail calls
                AND contract_address =  '\xcbcdf9626bc03e24f779434178a73a0b4bad62ed' --pool address
                -- and date_trunc('day', call_block_time) <= '2021-05-06'
                
                UNION ALL
                
                SELECT                          -- same to burn liquidity calls
                    (date_trunc('seconds', (call_block_time - timestamptz 'epoch') / 14400) * 14400 + timestamptz 'epoch') as tp,
                    "tickLower" AS lowerTick,
                    "tickUpper" AS UpperTick,
                    -amount AS amount
                FROM uniswap_v3."Pair_call_burn"
                WHERE call_success = true
                AND contract_address = '\xcbcdf9626bc03e24f779434178a73a0b4bad62ed'
                -- and date_trunc('day', call_block_time) <= '2021-05-06'
            )

, all_combs as ( -- This CTE generates all combinations of liquidity ranges and time periods by doing a cross join.
--All combinations are required because a particular range might not have a mint or burn on a certain day but does hold liquidity from a previous day.

--Ex: Liquidity amount of 100k is added to range 1211 to 1560 on date 1st June 2021. After this no liquidity was added to this range or removed. 
--So if we simply group by range and sum up liquidity for all days, 2nd June and onwards won't have a row for the range 1211 to 1560. The below cross join is done in order to have that row.
    select
        lowerTick,
        UpperTick,
        tp
    from (
            select
                lowerTick,
                UpperTick
            FROM mb
            group by 1,2
        )a
    cross join (
            SELECT tp
            FROM generate_series( '2021-05-04'::timestamp , current_timestamp, '4 hour'::interval) tp
            group by 1
        )b
                
)
            
, mint_burn as (
            select 
                tp,
                lowerTick,
                UpperTick,
                SUM(amount) over(partition by lowerTick, UpperTick order by tp asc) as amount -- This does a cumulative sum of liquidity betweeen 'lowerTick' and 'UpperTick'. So all mints and burns for that range get accounted for until each time period 'tp'
            from (
                    SELECT          -- sum liquidity for exact same range, added and removed liquidity will add to zero.
                        a.tp,
                        a.lowerTick,
                        a.UpperTick,
                        sum(coalesce(amount,0)) as amount
                    from all_combs a
                    left join mb b
                    on a.tp = b.tp
                    and a.lowerTick = b.lowerTick
                    and a.UpperTick = b.UpperTick
                    group by 1,2,3
                )a
            )
            
, ticks as (            
        SELECT              --for each range, create a series of ticks from lower to upper range. 
           tp,
           generate_series(lowerTick, UpperTick, 60) as tick, --Since this is a 0.3% fee pool, initialized ticks are multiples of 60
           amount AS amount_tick                             --distribute liquidity for each tick
        FROM mint_burn
        WHERE amount > 0 --remove ranges already that liquidity has been already removed
)

, liq as (
    SELECT 
        --1e12/(1.0001^tick) AS price,      --transform tick into price, used in previous query
        tp,
        tick,                           
        SUM(amount_tick) as liquidity       --add all liquidity for the same tick
    FROM ticks
    --WHERE tick BETWEEN 190000 AND 210000 --Do not filter table so above e below price liquidity get all data.
    GROUP BY 1,2
)

, prices AS (                     -- auxiliary table to get current price for the token
    SELECT
        minute,
        price,
        decimals,
        symbol
    from prices."usd"
    WHERE symbol = 'WBTC' --OR symbol = 'USDC'
)

, max_minute as (
    select 
    tp,
    max(minute) as minute --Getting the last price in time period 'tp'. This acts like closing price of a candlestick chart.
    from (
        SELECT
                minute,
                (date_trunc('seconds', (minute - timestamptz 'epoch') / 14400) * 14400 + timestamptz 'epoch') as tp
                
            from prices."usd"
            WHERE symbol = 'WBTC' --OR symbol = 'USDC'
            and date_trunc('day', minute) >= '2021-05-04'
            ORDER BY minute DESC
        ) a
    group by 1
)

, tp_price as (
    SELECT
        a.minute,
        tp,
        price,
        decimals,
        symbol
    from prices a
    inner join max_minute b
    on a.minute = b.minute
)



,
prices_eth AS (                     -- auxiliary table to get current price for the token
    SELECT
        minute,
        price,
        decimals,
        symbol
    from prices."usd"
    WHERE symbol = 'WETH' --OR symbol = 'USDC'
)

, max_minute_eth as (
    select 
    tp,
    max(minute) as minute --Getting the last price in time period 'tp'. This acts like closing price of a candlestick chart.
    from (
        SELECT
                minute,
                (date_trunc('seconds', (minute - timestamptz 'epoch') / 14400) * 14400 + timestamptz 'epoch') as tp
                
            from prices."usd"
            WHERE symbol = 'WETH' --OR symbol = 'USDC'
            and date_trunc('day', minute) >= '2021-05-04'
            ORDER BY minute DESC
        ) a
    group by 1
)

, tp_price_eth as (
    SELECT
        a.minute,
        tp,
        price,
        decimals,
        symbol
    from prices_eth a
    inner join max_minute_eth b
    on a.minute = b.minute
)

, tp_price_btceth as (
    SELECT
        a.tp,
        b.price/a.price as price --token1/token0
    from tp_price a
    inner join tp_price_eth b
    on a.tp = b.tp
)



/* Add metrics for each column
1- Current price
2- Sum all liquidity below current price
3- Sum liquidity between current_price/1.1 and current_price
4- Sum liquidity between current_price/1.05 and current_price
5- Sum liquidity between current_price and current_price*1.05
6- Sum liquidity between current_price/1.1 and current_price*1.1
7- Sum all liquidity above current price

Divide each metric by arbitrary number (1e18) to make it more suitable.
*/
-- select * from liq

SELECT
    a.*
    ,"Price-2%" + "Price+2%" AS "Price +/- 2%"
    ,"Above Price" + "Below Price" as "Total" --Total liquidity
    ,("Price-2%" + "Price+2%") / ("Above Price" + "Below Price") AS perc_within_2_percent
    ,("Price-5%" + "Price+5%") / ("Above Price" + "Below Price") AS perc_within_5_percent
    ,("Price-10%" + "Price+10%") / ("Above Price" + "Below Price") AS perc_within_10_percent
    ,("Price-2%" + "Price+2%") AS within_2_percent
    ,("Price-5%" + "Price+5%") AS within_5_percent
    ,("Price-10%" + "Price+10%") AS within_10_percent
FROM (
    SELECT
        liq.tp,
        cp.price AS current_price,
        SUM(CASE WHEN tick < log(1e10*cp.price)/log(1.0001) THEN liquidity END)/1e8 AS "Below Price",
        SUM(CASE WHEN tick BETWEEN log(1e10*cp.price/1.1)/log(1.0001) AND log(1e10*cp.price)/log(1.0001) THEN liquidity END)/1e8 AS "Price-10%",
        SUM(CASE WHEN tick BETWEEN log(1e10*cp.price/1.05)/log(1.0001) AND log(1e10*cp.price)/log(1.0001) THEN liquidity END)/1e8 AS "Price-5%",
        SUM(CASE WHEN tick BETWEEN log(1e10*cp.price)/log(1.0001) AND log(1e10*cp.price*1.05)/log(1.0001) THEN liquidity END)/1e8 AS "Price+5%",
        SUM(CASE WHEN tick BETWEEN log(1e10*cp.price)/log(1.0001) AND log(1e10*cp.price*1.1)/log(1.0001) THEN liquidity END)/1e8 AS "Price+10%",
        SUM(CASE WHEN tick > log(1e10*cp.price)/log(1.0001) THEN liquidity END)/1e8 AS "Above Price",
        SUM(CASE WHEN tick BETWEEN log(1e10*cp.price/1.02)/log(1.0001) AND log(1e10*cp.price)/log(1.0001) THEN liquidity END)/1e8 AS "Price-2%",
        SUM(CASE WHEN tick BETWEEN log(1e10*cp.price)/log(1.0001) AND log(1e10*cp.price*1.02)/log(1.0001) THEN liquidity END)/1e8 AS "Price+2%"
    FROM liq
    JOIN tp_price_btceth as cp ON cp.tp = liq.tp
    GROUP BY 1,2
    ) A
