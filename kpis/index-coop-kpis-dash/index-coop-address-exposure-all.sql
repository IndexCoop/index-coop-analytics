-- https://duneanalytics.com/queries/41628

WITH dpi_transfers AS (

  SELECT
    tr."from" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'

  UNION ALL

  SELECT
    tr."to" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'

),

dpi_balancer_add AS (

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

dpi_balancer_remove AS (

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

dpi_sushi_add AS (

  SELECT
    "to" AS address,
    ("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
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
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushi_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_addLiquidity"
  WHERE "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    OR "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'

),

dpi_sushi_remove AS (

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushi_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_removeLiquidityETH"
  WHERE token = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'

  UNION ALL

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
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
    date_trunc('day', call_block_time) AS evt_block_day,
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
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushi_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_removeLiquidityWithPermit"
  WHERE "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    OR "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'

),

dpi_uniswap_add AS (

  SELECT
    "to" AS address,
    ("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
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
    date_trunc('day', call_block_time) AS evt_block_day,
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
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_addLiquidity"
  WHERE "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    OR "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'

),

dpi_uniswap_remove AS (

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityETHWithPermit"
  WHERE token = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'

  UNION ALL

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
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
    date_trunc('day', call_block_time) AS evt_block_day,
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
    date_trunc('day', call_block_time) AS evt_block_day,
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
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityWithPermit"
  WHERE "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    OR "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'

),

dpi_cream_add AS (

    SELECT
      "minter" AS address,
      ("mintAmount"/1e18) AS amount,
      date_trunc('day', evt_block_time) AS evt_block_day,
      'cream_add' AS type,
      evt_tx_hash
    FROM creamfinance."CErc20Delegate_evt_Mint"
    WHERE contract_address = '\x2A537Fa9FFaea8C1A41D3C2B68a9cb791529366D'

),

dpi_cream_remove AS (

    SELECT
      "redeemer" AS address,
      -("redeemAmount"/1e18) AS amount,
      date_trunc('day', evt_block_time) AS evt_block_day,
      'cream_remove' AS type,
      evt_tx_hash
    FROM creamfinance."CErc20Delegate_evt_Redeem"
    WHERE contract_address = '\x2A537Fa9FFaea8C1A41D3C2B68a9cb791529366D'

),

dpi_lp AS (

  SELECT
    *
  FROM dpi_sushi_add

  UNION ALL

  SELECT
    *
  FROM dpi_sushi_remove

  UNION ALL

  SELECT
    *
  FROM dpi_uniswap_add

  UNION ALL

  SELECT
    *
  FROM dpi_uniswap_remove
  
  UNION ALL
  
  SELECT
    *
  FROM dpi_cream_add
  
  UNION ALL
  
  SELECT
    *
  FROM dpi_cream_remove
  
  UNION ALL
  
  SELECT
    *
  FROM dpi_balancer_add
  
  UNION ALL
  
  SELECT
    *
  FROM dpi_balancer_remove

),

dpi_contracts AS (

  SELECT
    address,
    "type"
  FROM labels.labels
  WHERE "type" = 'contract_name'

),

dpi_liquidity_providing AS (

  SELECT
    l.*,
    CASE c.type
      WHEN 'contract_name' THEN 'contract'
      ELSE 'non-contract'
    END AS contract
  FROM dpi_lp l
  LEFT JOIN dpi_contracts c ON l.address = c.address

),

dpi_moves AS (

  SELECT
    *
  FROM dpi_transfers

  UNION ALL

  SELECT
    address,
    amount,
    evt_block_day,
    type,
    evt_tx_hash
  FROM dpi_liquidity_providing
  WHERE contract = 'non-contract'

),

dpi_exposure AS (

    SELECT
      m.address,
      evt_block_day,
      sum(amount) AS exposure
    FROM dpi_moves m
    LEFT JOIN dpi_contracts c ON m.address = c.address
    WHERE c.type IS NULL
      AND m.type IN ('mint', 'burn', 'transfer',
      'uniswap_add', 'uniswap_remove', 'sushi_add', 'sushi_remove', 
      'cream_add', 'cream_remove', 'balancer_add', 'balancer_remove')
    GROUP BY 1, 2
    ORDER BY 1, 2

),

dpi_address_by_date  AS (

    SELECT
        DISTINCT
        t1.address,
        t2.evt_block_day
    FROM dpi_exposure t1
    CROSS JOIN (
        SELECT
            DISTINCT(evt_block_day)
        FROM dpi_exposure
    ) t2

),

dpi_temp AS (

  SELECT
    a.address,
    a.evt_block_day,
    CASE e.exposure
        WHEN NULL THEN 0
        ELSE e.exposure
    END AS exposure
  FROM dpi_address_by_date a
  LEFT JOIN dpi_exposure e ON a.address = e.address AND a.evt_block_day = e.evt_block_day

),

dpi_address_over_time AS (

    SELECT
        address,
        evt_block_day,
        sum(exposure) OVER (PARTITION BY address ORDER BY evt_block_day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exposure
    FROM dpi_temp

),

dpi AS (

    SELECT
        evt_block_day,
        'DPI' AS product,
        COUNT(DISTINCT(address))
    FROM dpi_address_over_time
    WHERE exposure > 0
    GROUP BY 1
    
),

-- ETH2x-FLI
fli_transfers AS (

  SELECT
    tr."from" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

  UNION ALL

  SELECT
    tr."to" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

),

fli_balancer_add AS (

  SELECT
    tr."from" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'balancer_add' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    AND evt_tx_hash IN (
    
        SELECT
            evt_tx_hash
        FROM balancer."BPool_evt_LOG_JOIN"
        WHERE "tokenIn" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
        
    )

),

fli_balancer_remove AS (

  SELECT
    tr."to" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'balancer_remove' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    AND evt_tx_hash IN (
    
        SELECT
            evt_tx_hash
        FROM balancer."BPool_evt_LOG_EXIT"
        WHERE "tokenOut" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
        
    )
    
),

fli_uniswap_add AS (

  SELECT
    "to" AS address,
    ("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_addLiquidityETH"
  WHERE token = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN ("output_amountA"/1e18)
      WHEN "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN ("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router01_call_addLiquidity"
  WHERE "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    OR "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN ("output_amountA"/1e18)
      WHEN "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN ("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_addLiquidity"
  WHERE "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    OR "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

),

fli_uniswap_remove AS (

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityETHWithPermit"
  WHERE token = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

  UNION ALL

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityETH"
  WHERE token = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router01_call_removeLiquidity"
  WHERE "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    OR "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidity"
  WHERE "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    OR "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityWithPermit"
  WHERE "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    OR "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

),

fli_lp AS (

SELECT * FROM fli_uniswap_add

UNION ALL

SELECT * FROM fli_uniswap_remove

UNION ALL

SELECT * FROM fli_balancer_add

UNION ALL

SELECT * FROM fli_balancer_remove

),

fli_contracts AS (

  SELECT
    address,
    "type"
  FROM labels.labels
  WHERE "type" = 'contract_name'

),

fli_liquidity_providing AS (

  SELECT
    l.*,
    CASE c.type
      WHEN 'contract_name' THEN 'contract'
      ELSE 'non-contract'
    END AS contract
  FROM fli_lp l
  LEFT JOIN fli_contracts c ON l.address = c.address

),

fli_moves AS (

  SELECT
    *
  FROM fli_transfers

  UNION ALL

  SELECT
    address,
    amount,
    evt_block_day,
    type,
    evt_tx_hash
  FROM fli_liquidity_providing
  WHERE contract = 'non-contract'

),

fli_exposure AS (

    SELECT
      m.address,
      evt_block_day,
      sum(amount) AS exposure
    FROM fli_moves m
    LEFT JOIN fli_contracts c ON m.address = c.address
    WHERE c.type IS NULL
      AND m.type IN ('mint', 'burn', 'transfer',
      'uniswap_add', 'uniswap_remove', 'balancer_add', 'balancer_remove')
    GROUP BY 1, 2
    ORDER BY 1, 2

),

fli_address_by_date  AS (

    SELECT
        DISTINCT
        t1.address,
        t2.evt_block_day
    FROM fli_exposure t1
    CROSS JOIN (
        SELECT
            DISTINCT(evt_block_day)
        FROM fli_exposure
    ) t2

),

fli_temp AS (

  SELECT
    a.address,
    a.evt_block_day,
    CASE e.exposure
        WHEN NULL THEN 0
        ELSE e.exposure
    END AS exposure
  FROM fli_address_by_date a
  LEFT JOIN fli_exposure e ON a.address = e.address AND a.evt_block_day = e.evt_block_day

),

fli_address_over_time AS (

    SELECT
        address,
        evt_block_day,
        sum(exposure) OVER (PARTITION BY address ORDER BY evt_block_day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exposure
    FROM fli_temp

),

fli AS (

    SELECT
        evt_block_day,
        'ETH2x-FLI' AS product,
        COUNT(DISTINCT(address))
    FROM fli_address_over_time
    WHERE exposure > 0
    GROUP BY 1

),

-- BTC2x-FLI
btc2x_transfers AS (

  SELECT
    tr."from" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'

  UNION ALL

  SELECT
    tr."to" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'

),

btc2x_balancer_add AS (

  SELECT
    tr."from" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'balancer_add' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
    AND evt_tx_hash IN (
    
        SELECT
            evt_tx_hash
        FROM balancer."BPool_evt_LOG_JOIN"
        WHERE "tokenIn" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
        
    )

),

btc2x_balancer_remove AS (

  SELECT
    tr."to" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'balancer_remove' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
    AND evt_tx_hash IN (
    
        SELECT
            evt_tx_hash
        FROM balancer."BPool_evt_LOG_EXIT"
        WHERE "tokenOut" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
        
    )
    
),

btc2x_sushi_add AS (

  SELECT
    "to" AS address,
    ("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushi_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_addLiquidityETH"
  WHERE token = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b' THEN ("output_amountA"/1e18)
      WHEN "tokenB" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b' THEN ("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushi_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_addLiquidity"
  WHERE "tokenA" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
    OR "tokenB" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'

),

btc2x_sushi_remove AS (

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushi_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_removeLiquidityETH"
  WHERE token = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'

  UNION ALL

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushi_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_removeLiquidityETHWithPermit"
  WHERE token = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushi_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_removeLiquidity"
  WHERE "tokenA" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
    OR "tokenB" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushi_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_removeLiquidityWithPermit"
  WHERE "tokenA" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
    OR "tokenB" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'

),

btc2x_lp AS (

SELECT * FROM btc2x_sushi_add

UNION ALL

SELECT * FROM btc2x_sushi_remove

UNION ALL

SELECT * FROM btc2x_balancer_add

UNION ALL

SELECT * FROM btc2x_balancer_remove

),

btc2x_contracts AS (

  SELECT
    address,
    "type"
  FROM labels.labels
  WHERE "type" = 'contract_name'

),

btc2x_liquidity_providing AS (

  SELECT
    l.*,
    CASE c.type
      WHEN 'contract_name' THEN 'contract'
      ELSE 'non-contract'
    END AS contract
  FROM btc2x_lp l
  LEFT JOIN btc2x_contracts c ON l.address = c.address

),

btc2x_moves AS (

  SELECT
    *
  FROM btc2x_transfers

  UNION ALL

  SELECT
    address,
    amount,
    evt_block_day,
    type,
    evt_tx_hash
  FROM btc2x_liquidity_providing
  WHERE contract = 'non-contract'

),

btc2x_exposure AS (

    SELECT
      m.address,
      evt_block_day,
      sum(amount) AS exposure
    FROM btc2x_moves m
    LEFT JOIN btc2x_contracts c ON m.address = c.address
    WHERE c.type IS NULL
      AND m.type IN ('mint', 'burn', 'transfer',
      'sushi_add', 'sushi_remove', 'balancer_add', 'balancer_remove')
    GROUP BY 1, 2
    ORDER BY 1, 2

),

btc2x_address_by_date  AS (

    SELECT
        DISTINCT
        t1.address,
        t2.evt_block_day
    FROM btc2x_exposure t1
    CROSS JOIN (
        SELECT
            DISTINCT(evt_block_day)
        FROM btc2x_exposure
    ) t2

),

btc2x_temp AS (

  SELECT
    a.address,
    a.evt_block_day,
    CASE e.exposure
        WHEN NULL THEN 0
        ELSE e.exposure
    END AS exposure
  FROM btc2x_address_by_date a
  LEFT JOIN btc2x_exposure e ON a.address = e.address AND a.evt_block_day = e.evt_block_day

),

btc2x_address_over_time AS (

    SELECT
        address,
        evt_block_day,
        sum(exposure) OVER (PARTITION BY address ORDER BY evt_block_day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exposure
    FROM btc2x_temp

),

btc2x AS (

    SELECT
        evt_block_day,
        'BTC2x-FLI' AS product,
        COUNT(DISTINCT(address))
    FROM btc2x_address_over_time
    WHERE exposure > 0
        AND evt_block_day >= '2021-05-11'
    GROUP BY 1

),

mvi_transfers AS (

  SELECT
    tr."from" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'

  UNION ALL

  SELECT
    tr."to" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'

),

mvi_uniswap_add AS (

  SELECT
    "to" AS address,
    ("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
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
    date_trunc('day', call_block_time) AS evt_block_day,
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
    date_trunc('day', call_block_time) AS evt_block_day,
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
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityETHWithPermit"
  WHERE token = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'

  UNION ALL

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
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
    date_trunc('day', call_block_time) AS evt_block_day,
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
    date_trunc('day', call_block_time) AS evt_block_day,
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
    date_trunc('day', call_block_time) AS evt_block_day,
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
    evt_block_day,
    type,
    evt_tx_hash
  FROM mvi_liquidity_providing
  WHERE contract = 'non-contract'

),

mvi_exposure AS (

    SELECT
      m.address,
      evt_block_day,
      sum(amount) AS exposure
    FROM mvi_moves m
    LEFT JOIN mvi_contracts c ON m.address = c.address
    WHERE c.type IS NULL
      AND m.type IN ('mint', 'burn', 'transfer',
      'uniswap_add', 'uniswap_remove')
    GROUP BY 1, 2
    ORDER BY 1, 2

),

mvi_address_by_date  AS (

    SELECT
        DISTINCT
        t1.address,
        t2.evt_block_day
    FROM mvi_exposure t1
    CROSS JOIN (
        SELECT
            DISTINCT(evt_block_day)
        FROM mvi_exposure
    ) t2

),

mvi_temp AS (

  SELECT
    a.address,
    a.evt_block_day,
    CASE e.exposure
        WHEN NULL THEN 0
        ELSE e.exposure
    END AS exposure
  FROM mvi_address_by_date a
  LEFT JOIN mvi_exposure e ON a.address = e.address AND a.evt_block_day = e.evt_block_day

),

mvi_address_over_time AS (

    SELECT
        address,
        evt_block_day,
        sum(exposure) OVER (PARTITION BY address ORDER BY evt_block_day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exposure
    FROM mvi_temp

),

mvi AS (

SELECT
    evt_block_day,
    'MVI' AS product,
    COUNT(DISTINCT(address))
FROM mvi_address_over_time
WHERE exposure > 0
GROUP BY 1

),

bed_transfers AS (

  SELECT
    tr."from" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x2af1df3ab0ab157e1e2ad8f88a7d04fbea0c7dc6'

  UNION ALL

  SELECT
    tr."to" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x2af1df3ab0ab157e1e2ad8f88a7d04fbea0c7dc6'

),

bed_uniswapv3_add as (

SELECT
	"from" as address,
	amount0 / 1e18 as amount,
	date_trunc('day', block_time) AS evt_block_day,
	'uniswapv3_add' as type,
	hash as evt_tx_hash
	
	FROM uniswap_v3."Pair_evt_Mint" m
	LEFT JOIN ethereum."transactions" tx ON m.evt_tx_hash = tx.hash
	WHERE tx.block_time > '7/21/21'
	and contract_address = '\x779DFffB81550BF503C19d52b1e91e9251234fAA'
	
),


bed_uniswapv3_remove as (

SELECT
	"from" as address,
	-amount0 / 1e18 as amount,
	date_trunc('day', block_time) AS evt_block_day,
	'uniswapv3_add' as type,
	hash as evt_tx_hash
	
	FROM uniswap_v3."Pair_evt_Burn" m
	LEFT JOIN ethereum."transactions" tx ON m.evt_tx_hash = tx.hash
	WHERE tx.block_time > '7/21/21'
	and contract_address = '\x779DFffB81550BF503C19d52b1e91e9251234fAA'
	
),


bed_lp AS (

SELECT * FROM bed_uniswapv3_add

UNION ALL

SELECT * FROM bed_uniswapv3_remove

),

bed_contracts AS (

  SELECT
    address,
    "type"
  FROM labels.labels
  WHERE "type" = 'contract_name'

),

bed_liquidity_providing AS (

  SELECT
    l.*,
    CASE c.type
      WHEN 'contract_name' THEN 'contract'
      ELSE 'non-contract'
    END AS contract
  FROM bed_lp l
  LEFT JOIN bed_contracts c ON l.address = c.address

),

bed_moves AS (

  SELECT
    *
  FROM bed_transfers

  UNION ALL

  SELECT
    address,
    amount,
    evt_block_day,
    type,
    evt_tx_hash
  FROM bed_liquidity_providing
  WHERE contract = 'non-contract'

),

bed_exposure AS (

    SELECT
      m.address,
      evt_block_day,
      sum(amount) AS exposure
    FROM bed_moves m
    LEFT JOIN bed_contracts c ON m.address = c.address
    WHERE c.type IS NULL
      AND m.type IN ('mint', 'burn', 'transfer', 'uniswapv3_add', 'uniswapv3_remove')
    GROUP BY 1, 2
    ORDER BY 1, 2

),

bed_address_by_date  AS (

    SELECT
        DISTINCT
        t1.address,
        t2.evt_block_day
    FROM bed_exposure t1
    CROSS JOIN (
        SELECT
            DISTINCT(evt_block_day)
        FROM bed_exposure
    ) t2

),

bed_temp AS (

  SELECT
    a.address,
    a.evt_block_day,
    CASE e.exposure
        WHEN NULL THEN 0
        ELSE e.exposure
    END AS exposure
  FROM bed_address_by_date a
  LEFT JOIN bed_exposure e ON a.address = e.address AND a.evt_block_day = e.evt_block_day

),

bed_address_over_time AS (

    SELECT
        address,
        evt_block_day,
        sum(exposure) OVER (PARTITION BY address ORDER BY evt_block_day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exposure
    FROM bed_temp

),

bed AS (

    SELECT
        evt_block_day,
        'BED' AS product,
        COUNT(DISTINCT(address))
    FROM bed_address_over_time
    WHERE exposure > 0
    GROUP BY 1

),

holders AS (

SELECT * FROM dpi

UNION ALL

SELECT * FROM fli

UNION ALL

SELECT * FROM btc2x

UNION ALL

SELECT * FROM mvi

UNION ALL

SELECT * FROM bed

)

SELECT
    evt_block_day,
    SUM(count) AS holders
FROM holders
GROUP BY 1
ORDER BY 1