-- https://duneanalytics.com/queries/45960

WITH transfers AS (

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

balancer_add AS (

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

balancer_remove AS (

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

sushi_add AS (

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

sushi_remove AS (

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

lp AS (

SELECT * FROM sushi_add

UNION ALL

SELECT * FROM sushi_remove

UNION ALL

SELECT * FROM balancer_add

UNION ALL

SELECT * FROM balancer_remove

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
    evt_block_day,
    type,
    evt_tx_hash
  FROM liquidity_providing
  WHERE contract = 'non-contract'

),

exposure AS (

    SELECT
      m.address,
      evt_block_day,
      sum(amount) AS exposure
    FROM moves m
    LEFT JOIN contracts c ON m.address = c.address
    WHERE c.type IS NULL
      AND m.type IN ('mint', 'burn', 'transfer',
      'sushi_add', 'sushi_remove', 'balancer_add', 'balancer_remove')
    GROUP BY 1, 2
    ORDER BY 1, 2

),

address_by_date  AS (

    SELECT
        DISTINCT
        t1.address,
        t2.evt_block_day
    FROM exposure t1
    CROSS JOIN (
        SELECT
            DISTINCT(evt_block_day)
        FROM exposure
    ) t2

),

temp AS (

  SELECT
    a.address,
    a.evt_block_day,
    CASE e.exposure
        WHEN NULL THEN 0
        ELSE e.exposure
    END AS exposure
  FROM address_by_date a
  LEFT JOIN exposure e ON a.address = e.address AND a.evt_block_day = e.evt_block_day

),

address_over_time AS (

    SELECT
        address,
        evt_block_day,
        sum(exposure) OVER (PARTITION BY address ORDER BY evt_block_day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exposure
    FROM temp

)

SELECT
    evt_block_day,
    COUNT(DISTINCT(address))
FROM address_over_time
WHERE exposure > 0
    AND evt_block_day >= '2021-05-11'
GROUP BY 1