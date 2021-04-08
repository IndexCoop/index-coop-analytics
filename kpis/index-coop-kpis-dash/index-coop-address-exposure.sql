-- https://duneanalytics.com/queries/25282/51827

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
    AND tr."to" = '\x0bcaea3571448877ff875bc3825ccf54e5d04df0'

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
    AND tr."from" = '\x0bcaea3571448877ff875bc3825ccf54e5d04df0'
    
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
    AND tr."to" = '\x83941a2d3cD426546eF4672376F6364fe69EeabD'

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
    AND tr."from" = '\x83941a2d3cD426546eF4672376F6364fe69EeabD'

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
        'ETH2X-FLI' AS product,
        COUNT(DISTINCT(address))
    FROM fli_address_over_time
    WHERE exposure > 0
    GROUP BY 1

),

cgi_transfers AS (

  SELECT
    tr."from" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'

  UNION ALL

  SELECT
    tr."to" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'

),

cgi_uniswap_add AS (

  SELECT
    "to" AS address,
    ("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_addLiquidityETH"
  WHERE token = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42' THEN ("output_amountA"/1e18)
      WHEN "tokenB" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42' THEN ("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router01_call_addLiquidity"
  WHERE "tokenA" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'
    OR "tokenB" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42' THEN ("output_amountA"/1e18)
      WHEN "tokenB" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42' THEN ("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_addLiquidity"
  WHERE "tokenA" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'
    OR "tokenB" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'

),

cgi_uniswap_remove AS (

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityETHWithPermit"
  WHERE token = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'

  UNION ALL

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityETH"
  WHERE token = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router01_call_removeLiquidity"
  WHERE "tokenA" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'
    OR "tokenB" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidity"
  WHERE "tokenA" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'
    OR "tokenB" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityWithPermit"
  WHERE "tokenA" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'
    OR "tokenB" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'

),

cgi_lp AS (

SELECT * FROM cgi_uniswap_add

UNION ALL

SELECT * FROM cgi_uniswap_remove

),

cgi_contracts AS (

  SELECT
    address,
    "type"
  FROM labels.labels
  WHERE "type" = 'contract_name'

),

cgi_liquidity_providing AS (

  SELECT
    l.*,
    CASE c.type
      WHEN 'contract_name' THEN 'contract'
      ELSE 'non-contract'
    END AS contract
  FROM cgi_lp l
  LEFT JOIN cgi_contracts c ON l.address = c.address

),

cgi_moves AS (

  SELECT
    *
  FROM cgi_transfers

  UNION ALL

  SELECT
    address,
    amount,
    evt_block_day,
    type,
    evt_tx_hash
  FROM cgi_liquidity_providing
  WHERE contract = 'non-contract'

),

cgi_exposure AS (

    SELECT
      m.address,
      evt_block_day,
      sum(amount) AS exposure
    FROM cgi_moves m
    LEFT JOIN cgi_contracts c ON m.address = c.address
    WHERE c.type IS NULL
      AND m.type IN ('mint', 'burn', 'transfer',
      'uniswap_add', 'uniswap_remove')
    GROUP BY 1, 2
    ORDER BY 1, 2

),

cgi_address_by_date  AS (

    SELECT
        DISTINCT
        t1.address,
        t2.evt_block_day
    FROM cgi_exposure t1
    CROSS JOIN (
        SELECT
            DISTINCT(evt_block_day)
        FROM cgi_exposure
    ) t2

),

cgi_temp AS (

  SELECT
    a.address,
    a.evt_block_day,
    CASE e.exposure
        WHEN NULL THEN 0
        ELSE e.exposure
    END AS exposure
  FROM cgi_address_by_date a
  LEFT JOIN cgi_exposure e ON a.address = e.address AND a.evt_block_day = e.evt_block_day

),

cgi_address_over_time AS (

    SELECT
        address,
        evt_block_day,
        sum(exposure) OVER (PARTITION BY address ORDER BY evt_block_day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exposure
    FROM cgi_temp

),

cgi AS (

    SELECT
        evt_block_day,
        'CGI' AS product,
        COUNT(DISTINCT(address))
    FROM cgi_address_over_time
    WHERE exposure > 0
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

)

SELECT * FROM dpi

UNION ALL

SELECT * FROM fli

UNION ALL

SELECT * FROM cgi

UNION ALL

SELECT * FROM mvi