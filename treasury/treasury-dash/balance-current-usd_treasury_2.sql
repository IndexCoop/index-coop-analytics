/*
    This should be identical to USD Treasury Balance 1
    with the query here: https://duneanalytics.com/queries/44939

    Except that it takes a different "end_date" parameter.
    The point of this is to show balances on two different days.
    Query for this one here: https://duneanalytics.com/queries/66423

    forked from https://duneanalytics.com/queries/22041/46378

    --- INDEX Treasury ---

    Wallet / Address
    ('\x9467cfadc9de245010df95ec6a585a506a8ad5fc', -- Treasury Wallet
    '\xe2250424378b6a6dC912f5714cfd308a8D593986', -- Treasury Committee Wallet
    '\x26e316f5b3819264DF013Ccf47989Fb8C891b088' -- Community Treasury Year 1 Vesting
    )    
    

*/

-- Start Generalized Price Feed block - see generalized_price_feed.sql
-- Modified price feed using EOD prices
with prices_by_minute as (
SELECT
        minute
        , symbol
        , decimals
        , price
        , row_number() over (partition by symbol, date_trunc('day', minute) order by minute desc) as row_num
        
    FROM prices.usd
    WHERE symbol in ('INDEX', 'DPI', 'MVI', 'ETH2x-FLI', 'BTC2x-FLI', 'USDC')
)
, prices_usd as (
    select date_trunc('day', minute) as dt
        , symbol
        , decimals
        , price -- Closing price at EOD UTC
    from prices_by_minute
    where row_num = 1
)
, eth_swaps AS (
    -- Uniswap price feed
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour
        , case 
            when contract_address = '\x3452A7f30A712e415a0674C0341d44eE9D9786F9' then 'INDEX'
            when contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991' then 'DPI'
            when contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff' then 'MVI'
            when contract_address = '\xf91c12dae1313d0be5d7a27aa559b1171cc1eac5' then 'ETH2x-FLI'
        end as symbol
        , ("amount0In" + "amount0Out")/1e18 AS a0_amt
        , ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM uniswap_v2."Pair_evt_Swap" sw
    WHERE contract_address in ( '\x3452A7f30A712e415a0674C0341d44eE9D9786F9' -- liq pair addresses I am searching the price for
                                , '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
                                , '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff' 
                                , '\xf91c12dae1313d0be5d7a27aa559b1171cc1eac5' )
        AND sw.evt_block_time >= '2020-09-10'
)
, btc_swaps as (   
    -- Sushi price feed
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour
        , 'BTC2x-FLI' as symbol
        , ("amount0In" + "amount0Out")/1e18 AS a0_amt
        , ("amount1In" + "amount1Out")/1e8 AS a1_amt
    FROM sushi."Pair_evt_Swap" sw
    WHERE contract_address = '\x164fe0239d703379bddde3c80e4d4800a1cd452b' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2021-05-11'

)

, swap_a1_eth_prcs AS (

    SELECT 
        avg(price) a1_prc
        , date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2020-09-10'
        AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --weth as base asset
    GROUP BY 2                
)

, swap_a1_btc_prcs as (

    SELECT 
        avg(price) a1_prc, 
        date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2021-05-11'
        AND contract_address ='\x2260fac5e5542a773aa44fbcfedf7c193bc2c599' --wbtc as base asset
    GROUP BY 2
)

, swap_hours AS (
    
    SELECT generate_series('2020-09-10 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
    
)
, eth_temp AS (

    SELECT
        h.hour
        , s.symbol
        , COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price
        -- , COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as asset_price
        -- a1_prcs."minute" AS minute
    FROM swap_hours h
    LEFT JOIN eth_swaps s ON h."hour" = s.hour 
    LEFT JOIN swap_a1_eth_prcs a ON h."hour" = a."hour"
    GROUP BY 1,2

) 
, btc_temp as (
    SELECT
        h.hour
        , s.symbol
        , COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price
        -- , COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as asset_price
        -- a1_prcs."minute" AS minute
    FROM swap_hours h
    LEFT JOIN btc_swaps s ON h."hour" = s.hour 
    LEFT JOIN swap_a1_btc_prcs a ON h."hour" = a."hour"
    GROUP BY 1,2
)
, swap_temp as (
    select * from eth_temp
    union
    select * from btc_temp
)
, swap_feed AS (
    SELECT
        hour
        , symbol
        , (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (PARTITION BY symbol ORDER BY hour), NULL))[COUNT(usd_price) OVER (PARTITION BY symbol ORDER BY hour)] AS usd_price
        -- , (ARRAY_REMOVE(ARRAY_AGG(asset_price) OVER (PARTITION BY symbol ORDER BY hour), NULL))[COUNT(asset_price) OVER (PARTITION BY symbol ORDER BY hour)] AS asset_price
    FROM swap_temp
)
, swap_price_feed_hour as (
    select hour
        , u.symbol
        , usd_price as price
        , row_number() over (partition by u.symbol, date_trunc('day', hour) order by hour desc) as row_num
    from swap_feed u
    left join prices_usd p on date_trunc('day', u.hour) = p.dt
        and u.symbol = p.symbol
    where p.dt is null
    and usd_price is not null
)
, swap_price_feed AS ( -- only include the uni feed when there's no corresponding price in prices_usd

    SELECT
        date_trunc('day', hour) AS dt
        , symbol
        , price
    FROM swap_price_feed_hour
    where row_num = 1

),
prices AS (

SELECT
    *
FROM prices_usd

UNION ALL

SELECT dt  
    , symbol
    , 18 as decimals -- all the INDEX tokens have 18 decimals
    , price
FROM swap_price_feed

)
-- End price feed block - output is CTE "prices"
, wallets AS (
    SELECT 'INDEX' AS org
        , '\x9467cfadc9de245010df95ec6a585a506a8ad5fc'::bytea AS address
        , 'Treasury Wallet' AS wallet
    /*UNION
    SELECT 'INDEX' AS org
        , '\xe2250424378b6a6dC912f5714cfd308a8D593986'::bytea AS address
        , 'Treasury Committee' AS wallet
    union
    select 'INDEX' AS org
    , '\x26e316f5b3819264DF013Ccf47989Fb8C891b088'::bytea AS address
    , 'Community Treasury Year 1 Vesting' AS wallet
    */
)

, creation_days AS (
    SELECT
        date_trunc('day', block_time) AS day
    FROM ethereum.traces
    WHERE address IN (SELECT address FROM wallets)
    AND TYPE = 'create'
)
, days AS (
    SELECT 
        generate_series(MIN(day), date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    FROM creation_days
)
, transfers AS (
    --ERC20 Tokens
    SELECT
        date_trunc('day', evt_block_time) AS day,
        "from" AS address,
        contract_address,
        sum(-value) AS amount
    FROM erc20."ERC20_evt_Transfer"
    WHERE "from" IN (SELECT address FROM wallets)
    AND evt_block_time >= (SELECT min(day) FROM creation_days)
    GROUP BY 1,2,3
    
    UNION ALL

    SELECT
        date_trunc('day', evt_block_time) AS day,
        "to" AS address,
        contract_address,
        sum(value) AS amount
    FROM erc20."ERC20_evt_Transfer"
    WHERE "to" IN (SELECT address FROM wallets)
    AND evt_block_time >= (SELECT min(day) FROM creation_days)
    GROUP BY 1,2,3
)

, decimals as (
    select distinct contract_address
    , decimals
    from prices.usd
    WHERE symbol in ('INDEX', 'DPI', 'MVI', 'ETH2x-FLI', 'BTC2x-FLI', 'USDC')
)

, transfers_day AS (
    SELECT
        t.day,
        t.address,
        t.contract_address,
        sum(t.amount/10^coalesce(d.decimals,18)) AS change 
    FROM transfers t
    left join decimals d on t.contract_address = d.contract_address
    GROUP BY 1,2,3
)

, balances_w_gap_days AS (
    SELECT
        day,
        address,
        contract_address,
        sum(change) OVER (PARTITION BY address, contract_address ORDER BY day) AS "balance",
        lead(day, 1, now()) OVER (PARTITION BY address, contract_address ORDER BY day) AS next_day
    FROM transfers_day
)

, balances_all_days AS (
    SELECT
        d.day,
--        b.address,
        b.contract_address,
        sum(b.balance) AS "balance"
    FROM balances_w_gap_days b
    INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
    GROUP BY 1,2 --,3
    ORDER BY 1,2 --,3
)
, usd_value_all_days as (
    SELECT
        b.day,
    --    b.address,
    --    w.wallet,
    --    w.org,
        b.contract_address,
        p.symbol AS token,
        b.balance,
        p.price,
        b.balance * coalesce(p.price,0) AS usd_value
        , rank() over (order by b.day desc)
    FROM balances_all_days b
    left join erc20.tokens t on b.contract_address = t.contract_address
    LEFT OUTER JOIN prices p ON t.symbol = p.symbol AND b.day = p.dt
    -- LEFT OUTER JOIN wallets w ON b.address = w.address
    where b.day <= '{{ end_date_2 }}'
    ORDER BY usd_value DESC
    LIMIT 10000
)
select contract_address
    , token
    , balance
    , usd_value
from usd_value_all_days
where rank = 1
;
