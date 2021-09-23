-- https://dune.xyz/queries/163047

WITH data_sync AS (

--eth/data sushi 0x208226200b45b82212b814f49efa643980a7bdd1
    
    SELECT
        date_trunc('hour', evt_block_time) AS hour,
        reserve1 / reserve0 AS eth_price,
        (reserve1 / reserve0) * p.price AS usd_price
    FROM sushi."Pair_evt_Sync" s
    LEFT JOIN prices.usd p ON p.symbol = 'WETH' AND p.minute = date_trunc('minute', s.evt_block_time)
    WHERE s.contract_address = '\x208226200b45b82212b814f49efa643980a7bdd1'
    ORDER BY 1

),

data_hours AS (
    
    SELECT generate_series('2021-09-21 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
    
),

data_temp AS (

    SELECT
        h.hour,
        COALESCE(AVG(usd_price), NULL) AS usd_price, 
        COALESCE(AVG(eth_price), NULL) as eth_price
    FROM data_hours h
    LEFT JOIN data_sync s ON s."hour" = h.hour 
    GROUP BY 1

),

data_feed AS (

    SELECT
        hour,
        'DATA' AS product,
        (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
        (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
    FROM data_temp

)

SELECT
    *
FROM data_feed
WHERE usd_price IS NOT NULL