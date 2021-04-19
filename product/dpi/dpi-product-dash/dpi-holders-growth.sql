-- https://duneanalytics.com/queries/33508

WITH dpi_user_base AS (

    WITH dpi_user_base AS (
    
        WITH transfers AS (
        
          SELECT
            tr."from" AS address,
            -tr.value / 1e18 AS amount,
            date_trunc('minute', evt_block_time) AS evt_block_minute,
            'transfer' AS type,
            evt_tx_hash
          FROM erc20."ERC20_evt_Transfer" tr
          WHERE contract_address = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
          UNION ALL
        
          SELECT
            tr."to" AS address,
            tr.value / 1e18 AS amount,
            date_trunc('minute', evt_block_time) AS evt_block_minute,
            'transfer' AS type,
            evt_tx_hash
          FROM erc20."ERC20_evt_Transfer" tr
          WHERE contract_address = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
        ),
        
        balancer_add AS (

          SELECT
            tr."from" AS address,
            tr.value / 1e18 AS amount,
            date_trunc('day', evt_block_time) AS evt_block_day,
            'balancer_add' AS type,
            evt_tx_hash
          FROM erc20."ERC20_evt_Transfer" tr
          WHERE contract_address = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
            AND evt_tx_hash IN (
            
                SELECT
                    evt_tx_hash
                FROM balancer."BPool_evt_LOG_JOIN"
                WHERE "tokenIn" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
                
            )

        ),

        balancer_remove AS (

          SELECT
            tr."to" AS address,
            -tr.value / 1e18 AS amount,
            date_trunc('day', evt_block_time) AS evt_block_day,
            'balancer_remove' AS type,
            evt_tx_hash
          FROM erc20."ERC20_evt_Transfer" tr
          WHERE contract_address = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
            AND evt_tx_hash IN (
            
                SELECT
                    evt_tx_hash
                FROM balancer."BPool_evt_LOG_EXIT"
                WHERE "tokenOut" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
                
            )
            
        ),
        
        sushi_add AS (
        
          SELECT
            "to" AS address,
            ("output_amountToken"/1e18) AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'sushi_add' AS type,
            call_tx_hash AS evt_tx_hash
          FROM sushi."Router02_call_addLiquidityETH"
          WHERE token = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            CASE
              WHEN "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN ("output_amountA"/1e18)
              WHEN "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN ("output_amountB"/1e18)
              ELSE 0
            END AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'sushi_add' AS type,
            call_tx_hash AS evt_tx_hash
          FROM sushi."Router02_call_addLiquidity"
          WHERE "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
            OR "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
        ),
        
        sushi_remove AS (
        
          SELECT
            "to" AS address,
            -("output_amountToken"/1e18) AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'sushi_remove' AS type,
            call_tx_hash AS evt_tx_hash
          FROM sushi."Router02_call_removeLiquidityETH"
          WHERE token = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            -("output_amountToken"/1e18) AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'sushi_remove' AS type,
            call_tx_hash AS evt_tx_hash
          FROM sushi."Router02_call_removeLiquidityETHWithPermit"
          WHERE token = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            CASE
              WHEN "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN -("output_amountA"/1e18)
              WHEN "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN -("output_amountB"/1e18)
              ELSE 0
            END AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'sushi_remove' AS type,
            call_tx_hash AS evt_tx_hash
          FROM sushi."Router02_call_removeLiquidity"
          WHERE "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
            OR "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            CASE
              WHEN "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN -("output_amountA"/1e18)
              WHEN "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN -("output_amountB"/1e18)
              ELSE 0
            END AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'sushi_remove' AS type,
            call_tx_hash AS evt_tx_hash
          FROM sushi."Router02_call_removeLiquidityWithPermit"
          WHERE "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
            OR "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
        ),
        
        uniswap_add AS (
        
          SELECT
            "to" AS address,
            ("output_amountToken"/1e18) AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_add' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router02_call_addLiquidityETH"
          WHERE token = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            CASE
              WHEN "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN ("output_amountA"/1e18)
              WHEN "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN ("output_amountB"/1e18)
              ELSE 0
            END AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_add' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router01_call_addLiquidity"
          WHERE "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
            OR "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            CASE
              WHEN "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN ("output_amountA"/1e18)
              WHEN "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN ("output_amountB"/1e18)
              ELSE 0
            END AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_add' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router02_call_addLiquidity"
          WHERE "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
            OR "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
        ),
        
        uniswap_remove AS (
        
          SELECT
            "to" AS address,
            -("output_amountToken"/1e18) AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_remove' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router02_call_removeLiquidityETHWithPermit"
          WHERE token = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            -("output_amountToken"/1e18) AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_remove' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router02_call_removeLiquidityETH"
          WHERE token = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            CASE
              WHEN "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN -("output_amountA"/1e18)
              WHEN "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN -("output_amountB"/1e18)
              ELSE 0
            END AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_remove' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router01_call_removeLiquidity"
          WHERE "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
            OR "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            CASE
              WHEN "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN -("output_amountA"/1e18)
              WHEN "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN -("output_amountB"/1e18)
              ELSE 0
            END AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_remove' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router02_call_removeLiquidity"
          WHERE "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
            OR "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
          UNION ALL
        
          SELECT
            "to" AS address,
            CASE
              WHEN "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN -("output_amountA"/1e18)
              WHEN "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN -("output_amountB"/1e18)
              ELSE 0
            END AS amount,
            date_trunc('minute', call_block_time) AS evt_block_minute,
            'uniswap_remove' AS type,
            call_tx_hash AS evt_tx_hash
          FROM uniswap_v2."Router02_call_removeLiquidityWithPermit"
          WHERE "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
            OR "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        
        ),
        
        cream_add AS (
        
            SELECT
              "minter" AS address,
              ("mintAmount"/1e18) AS amount,
              date_trunc('minute', evt_block_time),
              'cream_add' AS type,
              evt_tx_hash
            FROM creamfinance."CErc20Delegate_evt_Mint"
            WHERE contract_address = '\x2A537Fa9FFaea8C1A41D3C2B68a9cb791529366D'
        
        ),
        
        cream_remove AS (
        
            SELECT
              "redeemer" AS address,
              -("redeemAmount"/1e18) AS amount,
              date_trunc('minute', evt_block_time),
              'cream_remove' AS type,
              evt_tx_hash
            FROM creamfinance."CErc20Delegate_evt_Redeem"
            WHERE contract_address = '\x2A537Fa9FFaea8C1A41D3C2B68a9cb791529366D'
        
        ),
        
        lp AS (
        
          SELECT
            *
          FROM sushi_add
        
          UNION ALL
        
          SELECT
            *
          FROM sushi_remove
        
          UNION ALL
        
          SELECT
            *
          FROM uniswap_add
        
          UNION ALL
        
          SELECT
            *
          FROM uniswap_remove
          
          UNION ALL
          
          SELECT
            *
          FROM cream_add
          
          UNION ALL
          
          SELECT
            *
          FROM cream_remove
        
          UNION ALL
          
          SELECT
            *
          FROM balancer_add
          
          UNION ALL
          
          SELECT
            *
          FROM balancer_remove
        
        ),
        
        contracts AS (
        
          SELECT
            address,
            "type"
          FROM labels.labels
          WHERE "type" = 'contract_name'
        
        ),
        
        liquidity_providing AS (
        
          SELECT
            l.*,
            CASE c.type
              WHEN 'contract_name' THEN 'contract'
              ELSE 'non-contract'
            END AS contract
          FROM lp l
          LEFT JOIN contracts c ON l.address = c.address
        
        ),
        
        moves AS (
        
          SELECT
            *
          FROM transfers
        
          UNION ALL
        
          SELECT
            address,
            amount,
            evt_block_minute,
            type,
            evt_tx_hash
          FROM liquidity_providing
          WHERE contract = 'non-contract'
        
        ),
        
        actions AS (
        
            SELECT
              m.address,
              m.evt_block_minute,
              m.amount,
              m.type,
              m.evt_tx_hash
            FROM moves m
            LEFT JOIN contracts c ON m.address = c.address
            WHERE c.type IS NULL
              AND m.type IN ('mint', 'burn', 'transfer',
              'uniswap_add', 'uniswap_remove', 'sushi_add', 'sushi_remove', 
              'cream_add', 'cream_remove', 'balancer_add', 'balancer_remove')
        
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
            FROM dpi_user_base
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
        FROM dpi_user_base
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
        FROM dpi_user_base
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
    
)

SELECT
    exposure,
    cohort,
    MIN(dt) AS min_dt,
    COUNT(DISTINCT address) AS addresses
FROM dpi_user_base
GROUP BY 1, 2
ORDER BY 3




