-- query at https://duneanalytics.com/queries/77403

-- copied from https://duneanalytics.com/queries/25282

-- index token address: '\x0954906da0Bf32d5479e25f46056d22f08464cab'

WITH index_transfers AS (

  SELECT
    tr."from" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

  UNION ALL

  SELECT
    tr."to" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

),

index_balancer_add AS (

  SELECT
    tr."from" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'balancer_add' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
    AND evt_tx_hash IN (
    
        SELECT
            evt_tx_hash
        FROM balancer."BPool_evt_LOG_JOIN"
        WHERE "tokenIn" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
        
    )

),

index_balancer_remove AS (

  SELECT
    tr."to" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'balancer_remove' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
    AND evt_tx_hash IN (
    
        SELECT
            evt_tx_hash
        FROM balancer."BPool_evt_LOG_EXIT"
        WHERE "tokenOut" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
        
    )
    
),

index_sushi_add AS (

  SELECT
    "to" AS address,
    ("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushi_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_addLiquidityETH"
  WHERE token = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN ("output_amountA"/1e18)
      WHEN "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN ("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushi_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_addLiquidity"
  WHERE "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
    OR "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

),

index_sushi_remove AS (

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushi_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_removeLiquidityETH"
  WHERE token = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

  UNION ALL

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushi_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_removeLiquidityETHWithPermit"
  WHERE token = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushi_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_removeLiquidity"
  WHERE "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
    OR "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushi_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_removeLiquidityWithPermit"
  WHERE "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
    OR "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

),

index_uniswap_add AS (

  SELECT
    "to" AS address,
    ("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_addLiquidityETH"
  WHERE token = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN ("output_amountA"/1e18)
      WHEN "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN ("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router01_call_addLiquidity"
  WHERE "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
    OR "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN ("output_amountA"/1e18)
      WHEN "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN ("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_addLiquidity"
  WHERE "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
    OR "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

),

index_uniswap_remove AS (

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityETHWithPermit"
  WHERE token = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

  UNION ALL

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityETH"
  WHERE token = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router01_call_removeLiquidity"
  WHERE "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
    OR "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidity"
  WHERE "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
    OR "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityWithPermit"
  WHERE "tokenA" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
    OR "tokenB" = '\x0954906da0Bf32d5479e25f46056d22f08464cab'

),

index_uniswapv3_add as (

SELECT
	"from" as address,
	amount0 / 1e18 as amount,
	date_trunc('day', block_time) AS evt_block_day,
	'uniswapv3_add' as type,
	hash as evt_tx_hash
	
	FROM uniswap_v3."Pair_evt_Mint" m
	LEFT JOIN ethereum."transactions" tx ON m.evt_tx_hash = tx.hash
	WHERE tx.block_time > '5/4/21'
	and contract_address = '\x8c13148228765Ba9E84EAf940b0416a5e349A5e7' -- the Uni v3 contract address
  
	
),


index_uniswapv3_remove as (

SELECT
	"from" as address,
	-amount0 / 1e18 as amount,
	date_trunc('day', block_time) AS evt_block_day,
	'uniswapv3_remove' as type,
	hash as evt_tx_hash
	
	FROM uniswap_v3."Pair_evt_Burn" m
	LEFT JOIN ethereum."transactions" tx ON m.evt_tx_hash = tx.hash
	WHERE tx.block_time > '5/4/21'
	and contract_address = '\x8c13148228765Ba9E84EAf940b0416a5e349A5e7' -- the Uni v3 contract address
	
),


index_lp AS (

  SELECT * FROM index_sushi_add
  UNION ALL
  SELECt * FROM index_sushi_remove
  UNION ALL
  SELECT * FROM index_uniswap_add
  UNION ALL
  SELECT * FROM index_uniswap_remove
  UNION ALL
  SELECT * FROM index_balancer_add
  UNION ALL
  SELECT * FROM index_balancer_remove
  union all
  select * from index_uniswapv3_add
  union all
  select * from index_uniswapv3_remove

),

index_liquidity_providing AS (

  SELECT
    l.*,
    CASE c.type
      WHEN 'contract_name' THEN 'contract'
      ELSE 'non-contract'
    END AS contract
  FROM index_lp l
  LEFT JOIN labels.labels c ON l.address = c.address

),

index_moves AS (

  SELECT
    *
  FROM index_transfers

  UNION ALL

  SELECT
    address,
    amount,
    evt_block_day,
    type,
    evt_tx_hash
  FROM index_liquidity_providing
  WHERE contract = 'non-contract'

),

index_exposure AS (

    SELECT
      m.address,
      evt_block_day,
      sum(amount) AS exposure
    FROM index_moves m
    LEFT JOIN labels.labels c ON m.address = c.address
    WHERE c.type IS NULL
      AND m.type IN ('mint', 'burn', 'transfer',
      'uniswap_add', 'uniswap_remove', 'sushi_add', 'sushi_remove', 
      'uniswapv3_add', 'uniswapv3_remove', 'balancer_add', 'balancer_remove')
    GROUP BY 1, 2
    ORDER BY 1, 2

),

index_address_by_date  AS (

    SELECT
        DISTINCT
        t1.address,
        t2.evt_block_day
    FROM index_exposure t1
    CROSS JOIN (
        SELECT
            DISTINCT(evt_block_day)
        FROM index_exposure
    ) t2

),

index_temp AS (

  SELECT
    a.address,
    a.evt_block_day,
    coalesce(e.exposure,0) AS exposure
  FROM index_address_by_date a
  LEFT JOIN index_exposure e ON a.address = e.address AND a.evt_block_day = e.evt_block_day

),

index_address_over_time AS (

    SELECT
        address,
        evt_block_day,
        sum(exposure) OVER (PARTITION BY address ORDER BY evt_block_day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exposure
    FROM index_temp

)
    SELECT
        evt_block_day,
        'INDEX' AS product,
        COUNT(DISTINCT(address))
    FROM index_address_over_time
    WHERE exposure > 0
    GROUP BY 1
