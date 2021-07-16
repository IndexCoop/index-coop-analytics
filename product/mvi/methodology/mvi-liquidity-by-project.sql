-- https://duneanalytics.com/queries/24742

WITH uniswap_pairs AS (

  SELECT
    token0,
    erc20.decimals as decimals0,
    erc20.symbol as symbol0,
    token1,
    erc202.decimals as decimals1,
    erc202.symbol as symbol1,
    pair
  FROM uniswap_v2."Factory_evt_PairCreated" pairsraw
  LEFT JOIN erc20.tokens erc20 ON pairsraw.token0 = erc20.contract_address
  LEFT JOIN erc20.tokens erc202 ON pairsraw.token1 = erc202.contract_address
  WHERE erc20.symbol IN ('MANA', 'ENJ', 'SAND', 'AXS', 'WHALE', 'NFTX', 
  'AUDIO', 'RFOX', 'REVV', 'RARI', 'MEME', 'TVK', '$DG', 'GALA', 'MUSE', 'WAXE', 'CHZ', 'ERN', 'ILV') OR
    erc202.symbol IN ('MANA', 'ENJ', 'SAND', 'AXS', 'WHALE', 'NFTX', 
  'AUDIO', 'RFOX', 'REVV', 'RARI', 'MEME', 'TVK', '$DG', 'GALA', 'MUSE', 'WAXE', 'CHZ', 'ERN', 'ILV')
  
),

uniswap_reserves AS (

  SELECT
    AVG(reserve0) AS reserve0,
    AVG(reserve1) AS reserve1,
    contract_address,
    date_trunc('day', evt_block_time) AS dt
  FROM uniswap_v2."Pair_evt_Sync" sync
  WHERE contract_address IN (SELECT DISTINCT pair FROM uniswap_pairs)
    AND date_trunc('day', evt_block_time) > (NOW() - interval '90' day)
  GROUP BY 3, 4

),

uniswap_liquidity AS (

  SELECT
      r.*,
      p.*,
      r.reserve0 / 10^p.decimals0 AS amount0,
      r.reserve1 / 10^p.decimals1 AS amount1,
      u0.price AS token0_price,
      u1.price AS token1_price
  FROM uniswap_reserves r
  INNER JOIN uniswap_pairs p
      ON r.contract_address = p.pair
  LEFT JOIN prices.usd u0
      ON p.token0 = u0.contract_address AND r.dt = u0.minute
  LEFT JOIN prices.usd u1
      ON p.token1 = u1.contract_address AND r.dt = u1.minute

),

uniswap_token0 AS (

  SELECT
    dt,
    pair,
    symbol0 AS symbol,
    token0 AS token,
    amount0 AS amount,
    token0_price AS price,
    CASE
      WHEN token0_price IS NOT NULL THEN token0_price * amount0 * 2
      WHEN token1_price IS NOT NULL THEN token1_price * amount1 * 2
      ELSE NULL
    END AS liquidity
  FROM uniswap_liquidity

),

uniswap_token1 AS (

  SELECT
    dt,
    pair,
    symbol1 AS symbol,
    token1 AS token,
    amount1 AS amount,
    token1_price AS price,
    CASE
      WHEN token1_price IS NOT NULL THEN token1_price * amount1 * 2
      WHEN token0_price IS NOT NULL THEN token0_price * amount0 * 2
      ELSE NULL
    END AS liquidity
  FROM uniswap_liquidity

),

uniswap_token_liquidity AS (

  SELECT
    *
  FROM uniswap_token0

  UNION

  SELECT
    *
  FROM uniswap_token1

),

uniswap AS (

SELECT
    dt,
    token,
    symbol,
    'uniswap' AS project,
    SUM(liquidity) AS liquidity
FROM uniswap_token_liquidity
WHERE symbol IN ('MANA', 'ENJ', 'SAND', 'AXS', 'WHALE', 'NFTX', 
  'AUDIO', 'RFOX', 'REVV', 'RARI', 'MEME', 'TVK', '$DG', 'GALA', 'MUSE', 'WAXE', 'CHZ', 'ERN', 'ILV')
GROUP BY 1, 2, 3
ORDER BY 2, 1

),

sushiswap_pairs AS (

  SELECT
    token0,
    erc20.decimals as decimals0,
    erc20.symbol as symbol0,
    token1,
    erc202.decimals as decimals1,
    erc202.symbol as symbol1,
    pair
  FROM sushi."Factory_evt_PairCreated" pairsraw
  LEFT JOIN erc20.tokens erc20 ON pairsraw.token0 = erc20.contract_address
  LEFT JOIN erc20.tokens erc202 ON pairsraw.token1 = erc202.contract_address
  WHERE erc20.symbol IN ('MANA', 'ENJ', 'SAND', 'AXS', 'WHALE', 'NFTX', 
  'AUDIO', 'RFOX', 'REVV', 'RARI', 'MEME', 'TVK', '$DG', 'GALA', 'MUSE', 'WAXE', 'CHZ', 'ERN', 'ILV') OR
    erc202.symbol IN ('MANA', 'ENJ', 'SAND', 'AXS', 'WHALE', 'NFTX', 
  'AUDIO', 'RFOX', 'REVV', 'RARI', 'MEME', 'TVK', '$DG', 'GALA', 'MUSE', 'WAXE', 'CHZ', 'ERN', 'ILV')
  
),

sushiswap_reserves AS (

  SELECT
    AVG(reserve0) AS reserve0,
    AVG(reserve1) AS reserve1,
    contract_address,
    date_trunc('day', evt_block_time) AS dt
  FROM sushi."Pair_evt_Sync" sync
  WHERE contract_address IN (SELECT DISTINCT pair FROM sushiswap_pairs)
    AND date_trunc('day', evt_block_time) > (NOW() - interval '90' day)
  GROUP BY 3, 4

),

sushiswap_liquidity AS (

  SELECT
      r.*,
      p.*,
      r.reserve0 / 10^p.decimals0 AS amount0,
      r.reserve1 / 10^p.decimals1 AS amount1,
      u0.price AS token0_price,
      u1.price AS token1_price
  FROM sushiswap_reserves r
  INNER JOIN sushiswap_pairs p
      ON r.contract_address = p.pair
  LEFT JOIN prices.usd u0
      ON p.token0 = u0.contract_address AND r.dt = u0.minute
  LEFT JOIN prices.usd u1
      ON p.token1 = u1.contract_address AND r.dt = u1.minute

),

sushiswap_token0 AS (

  SELECT
    dt,
    pair,
    symbol0 AS symbol,
    token0 AS token,
    amount0 AS amount,
    token0_price AS price,
    CASE
      WHEN token0_price IS NOT NULL THEN token0_price * amount0 * 2
      WHEN token1_price IS NOT NULL THEN token1_price * amount1 * 2
      ELSE NULL
    END AS liquidity
  FROM sushiswap_liquidity

),

sushiswap_token1 AS (

  SELECT
    dt,
    pair,
    symbol1 AS symbol,
    token1 AS token,
    amount1 AS amount,
    token1_price AS price,
    CASE
      WHEN token1_price IS NOT NULL THEN token1_price * amount1 * 2
      WHEN token0_price IS NOT NULL THEN token0_price * amount0 * 2
      ELSE NULL
    END AS liquidity
  FROM sushiswap_liquidity

),

sushiswap_token_liquidity AS (

  SELECT
    *
  FROM sushiswap_token0

  UNION

  SELECT
    *
  FROM sushiswap_token1

),

sushiswap AS (

SELECT
    dt,
    token,
    symbol,
    'sushiswap' AS project,
    SUM(liquidity) AS liquidity
FROM sushiswap_token_liquidity
WHERE symbol IN ('MANA', 'ENJ', 'SAND', 'AXS', 'WHALE', 'NFTX', 
  'AUDIO', 'RFOX', 'REVV', 'RARI', 'MEME', 'TVK', '$DG', 'GALA', 'MUSE', 'WAXE', 'CHZ', 'ERN', 'ILV')
GROUP BY 1, 2, 3
ORDER BY 2, 1

),

balancer_pools AS (

SELECT 
day,
token,
pool,
cumulative_amount,
cumulative_amount / 10^erc20.decimals AS transformed_amount,
p.price,
cumulative_amount / 10^erc20.decimals * p.price AS usd_amount,
erc20.symbol
FROM balancer."view_balances" a
LEFT JOIN erc20.tokens erc20 ON a.token = erc20.contract_address
LEFT JOIN prices.usd p ON a.token = p.contract_address
    AND p.minute = date_trunc('minute', a.day)
WHERE token IN (
'\x0f5d2fb29fb7d3cfee444a200298f468908cc942',
'\xf629cbd94d3791c9250152bd8dfbdf380e2a3b9c',
'\x3845badAde8e6dFF049820680d1F14bD3903a5d0',
'\xbb0e17ef65f82ab018d8edd776e8dd940327b28b',
'\x9355372396e3F6daF13359B7b607a3374cc638e0',
'\x87d73E916D7057945c9BcD8cdd94e42A6F47f776',
'\x18aAA7115705e8be94bfFEBDE57Af9BFc265B998',
'\xa1d6df714f91debf4e0802a542e13067f31b8262',
'\x557B933a7C2c45672B610F8954A3deB39a51A8Ca',
'\xfca59cd816ab1ead66534d82bc21e7515ce441cf',
'\xD5525D397898e5502075Ea5E830d8914f6F0affe',
'\xd084b83c305dafd76ae3e1b4e1f1fe2ecccb3988',
'\xEE06A81a695750E71a662B51066F2c74CF4478a0',
'\x15D4c048F83bd7e37d49eA4C83a07267Ec4203dA',
'\xb6ca7399b4f9ca56fc27cbff44f4d2e4eef1fc81',
'\x7a2Bc711E19ba6aff6cE8246C546E8c4B4944DFD',
'\x3506424f91fd33084466f402d5d97f05f8e3b4af',
'\xbbc2ae13b23d715c30720f079fcd9b4a74093505',
'\x767fe9edc9e0df98e07454847909b5e959d7ca0e'
)
    AND cumulative_amount / 10^erc20.decimals * p.price IS NOT NULL
    AND day > (NOW() - interval '90' day)

),

balancer AS (

SELECT
day AS dt,
token,
symbol,
'balancer' AS project,
SUM(usd_amount) AS liquidity
FROM balancer_pools
GROUP BY 1, 2, 3, 4

),

uni_sushi_balancer AS (

    SELECT
        *
    FROM uniswap
    
    UNION
    
    SELECT
        *
    FROM sushiswap
    
    UNION
    
    SELECT
        *
    FROM balancer

)

SELECT
    *
FROM uni_sushi_balancer
WHERE token != '\x53c8395465a84955c95159814461466053dedede'
ORDER BY symbol, project, dt