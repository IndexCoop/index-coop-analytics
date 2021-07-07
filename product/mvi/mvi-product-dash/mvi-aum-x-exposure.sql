-- https://duneanalytics.com/queries/42170

WITH mvi_user_base AS (

    WITH mvi_user_base AS (
    
        WITH mvi_transfers AS (
    
          SELECT
            tr."from" AS address,
            -tr.value / 1e18 AS amount,
            date_trunc('minute', evt_block_time) AS evt_block_minute,
            'transfer' AS type,
            evt_tx_hash
          FROM erc20."ERC20_evt_Transfer" tr
          WHERE contract_address = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
        
          UNION ALL
        
          SELECT
            tr."to" AS address,
            tr.value / 1e18 AS amount,
            date_trunc('minute', evt_block_time) AS evt_block_minute,
            'transfer' AS type,
            evt_tx_hash
          FROM erc20."ERC20_evt_Transfer" tr
          WHERE contract_address = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
        
        ),
        
        mvi_uniswap_add AS (
        
          SELECT
            "to" AS address,
            ("output_amountToken"/1e18) AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_add' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router02_call_addLiquidityETH"
          WHERE token = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            CASE
              WHEN "tokenA" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7' THEN ("output_amountA"/1e18)
              WHEN "tokenB" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7' THEN ("output_amountB"/1e18)
              ELSE 0
            END AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_add' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router01_call_addLiquidity"
          WHERE "tokenA" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
            OR "tokenB" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            CASE
              WHEN "tokenA" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7' THEN ("output_amountA"/1e18)
              WHEN "tokenB" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7' THEN ("output_amountB"/1e18)
              ELSE 0
            END AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_add' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router02_call_addLiquidity"
          WHERE "tokenA" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
            OR "tokenB" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
        
        ),
        
        mvi_uniswap_remove AS (
        
          SELECT
            "to" AS address,
            -("output_amountToken"/1e18) AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_remove' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router02_call_removeLiquidityETHWithPermit"
          WHERE token = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            -("output_amountToken"/1e18) AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_remove' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router02_call_removeLiquidityETH"
          WHERE token = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            CASE
              WHEN "tokenA" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7' THEN -("output_amountA"/1e18)
              WHEN "tokenB" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7' THEN -("output_amountB"/1e18)
              ELSE 0
            END AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_remove' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router01_call_removeLiquidity"
          WHERE "tokenA" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
            OR "tokenB" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            CASE
              WHEN "tokenA" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7' THEN -("output_amountA"/1e18)
              WHEN "tokenB" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7' THEN -("output_amountB"/1e18)
              ELSE 0
            END AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_remove' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router02_call_removeLiquidity"
          WHERE "tokenA" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
            OR "tokenB" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            CASE
              WHEN "tokenA" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7' THEN -("output_amountA"/1e18)
              WHEN "tokenB" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7' THEN -("output_amountB"/1e18)
              ELSE 0
            END AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_remove' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router02_call_removeLiquidityWithPermit"
          WHERE "tokenA" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
            OR "tokenB" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
        
        ),
        
        mvi_lp AS (
        
        SELECT * FROM mvi_uniswap_add
        
        UNION ALL
        
        SELECT * FROM mvi_uniswap_remove
        
        ),
        
        mvi_contracts AS (
        
          SELECT
            address,
            "type"
          FROM labels.labels
          WHERE "type" = 'contract_name'
        
        ),
        
        mvi_liquidity_providing AS (
        
          SELECT
            l.*,
            CASE c.type
              WHEN 'contract_name' THEN 'contract'
              ELSE 'non-contract'
            END AS contract
          FROM mvi_lp l
          LEFT JOIN mvi_contracts c ON l.address = c.address
        
        ),
        
        mvi_moves AS (
        
          SELECT
            *
          FROM mvi_transfers
        
          UNION ALL
        
          SELECT
            address,
            amount,
            evt_block_minute,
            type,
            evt_tx_hash
          FROM mvi_liquidity_providing
          WHERE contract = 'non-contract'
        
        ),
        
        actions AS (
        
            SELECT
              m.address,
              m.evt_block_minute,
              m.amount,
              m.type,
              m.evt_tx_hash
            FROM mvi_moves m
            LEFT JOIN mvi_contracts c ON m.address = c.address
            WHERE c.type IS NULL
              AND m.type IN ('mint', 'burn', 'transfer',
              'uniswap_add', 'uniswap_remove')
        
        )
        
        SELECT
          *
        FROM actions
        WHERE address != '\x0000000000000000000000000000000000000000'
    
    ),
    
    contract_bots AS (
    
        WITH contract_bots_temp AS (
        
            SELECT
                address,
                date_trunc('day', evt_block_minute),
                SUM(amount) AS amount,
                COUNT(DISTINCT evt_tx_hash) AS n_tx_hash,
                COUNT(*) AS n_movements
            FROM mvi_user_base
            GROUP BY 1, 2
        
        )
        
        SELECT
            DISTINCT
            address
        FROM contract_bots_temp
        WHERE amount <= 1e-14 AND n_movements >= 2
    
    ),
    
    good_addresses AS (
    
        SELECT
            DISTINCT
            address
        FROM mvi_user_base
        WHERE address NOT IN (SELECT address FROM contract_bots)
    
    ),
    
    temp AS (
    
        SELECT
            address,
            evt_block_minute AS dt,
            amount,
            type,
            evt_tx_hash,
            SUM(amount) OVER (PARTITION BY address ORDER BY evt_block_minute) AS running_exposure
        FROM mvi_user_base
        WHERE address NOT IN (SELECT address FROM contract_bots)
        ORDER BY 1, 2
    
    ),
    
    cohorts AS (
    
        SELECT
            address,
            date_trunc('day', MIN(dt)) AS start_dt,
            to_char(MIN(dt), 'Mon') || ' ' || date_part('year', MIN(dt)) AS cohort,
            CASE 
                WHEN MAX(running_exposure) >= 250 THEN '250+'
                WHEN MAX(running_exposure) >= 50 THEN '50-249'
                WHEN MAX(running_exposure) >= 10 THEN '10-49'
                ELSE '<10'
            END AS exposure
        FROM temp
        GROUP BY 1
    
    ),
    
    cohorts_raw AS (
        
        SELECT
            dt,
            to_char(dt, 'Mon') || ' ' || date_part('year', dt) AS cohort
        FROM temp
        ORDER BY dt
    
    ),
    
    cohort_levels AS (
    
        SELECT
            DISTINCT cohort
        FROM cohorts_raw
    
    ),
    
    current_cohort AS (
    
        SELECT
            to_char(MIN(CURRENT_DATE), 'Mon') || ' ' || date_part('year', MIN(CURRENT_DATE)) AS cohort
            
    ),
    
    completed_cohorts AS (
    
        SELECT
            *
        FROM cohort_levels
        WHERE cohort NOT IN (SELECT cohort FROM current_cohort)
    
    ),
    
    full_address_dates AS (
    
        SELECT
            address,
            dt
        FROM good_addresses
        CROSS JOIN generate_series('2020-09-10'::date, date_trunc('day', NOW()), '1 day') AS dt
        
    ),
    
    address_dates AS (
    
        SELECT
            t.*
        FROM full_address_dates t
        LEFT JOIN cohorts c ON t.address = c.address
        WHERE t.dt >= c.start_dt
        
    ),
    
    address_date_amount AS (
    
        SELECT
            a.*,
            COALESCE(t.amount, 0) AS amount
        FROM address_dates a
        LEFT JOIN (
            SELECT
                address,
                date_trunc('day', dt) AS dt,
                SUM(amount) AS amount
            FROM temp
            GROUP BY 1, 2
        ) t ON a.address = t.address AND a.dt = t.dt
    
    ),
    
    address_daily_balance AS (
    
        SELECT
            *,
            SUM(amount) OVER (PARTITION BY address ORDER BY dt) AS running_amount,
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY dt) AS day
        FROM address_date_amount
    
    ),
    
    fin AS (
    
        SELECT
            a.*,
            CASE
                WHEN a.running_amount > 0 THEN 1
                ELSE 0
            END AS retained,
            c.cohort,
            c.exposure
        FROM address_daily_balance a
        LEFT JOIN cohorts c ON a.address = c.address
    
    ),
    
    include_days AS (
    
        SELECT 
            cohort,
            ROUND(MAX(day) * 1) AS include_days
        FROM fin
        GROUP BY 1
    
    ),
    
    final AS (
    
        SELECT
            a.*
        FROM fin a
        LEFT JOIN include_days b ON a.cohort = b.cohort
        WHERE b.include_days >= a.day
    
    )
    
    SELECT * FROM final

),

mvi_price_feed AS (

    WITH prices_usd AS (
    
        SELECT
            date_trunc('day', minute) AS dt,
            AVG(price) AS price
        FROM prices.usd
        WHERE symbol = 'MVI'
        GROUP BY 1
        ORDER BY 1
        
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
        WHERE date_trunc('day', hour) NOT IN (SELECT dt FROM prices_usd)
            AND usd_price IS NOT NULL
        GROUP BY 1
    
    ),
    
    mvi_price AS (
    
        SELECT
            *
        FROM prices_usd
        
        UNION ALL
        
        SELECT
            *
        FROM mvi_price_feed

    )

SELECT
    *
FROM mvi_price
WHERE dt > '2021-04-01'
ORDER BY 1

)

SELECT
    a.exposure,
    a.dt,
    SUM(a.running_amount) AS units,
    SUM(a.running_amount) * AVG(p.price) AS aum
FROM mvi_user_base a
LEFT JOIN mvi_price_feed p ON a.dt = p.dt
WHERE retained = 1
GROUP BY 1, 2



