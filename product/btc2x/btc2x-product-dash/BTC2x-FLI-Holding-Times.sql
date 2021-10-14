
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


-- BTC2x-FLI is on SushiSwap, which only has Router02 contract
sushiswap_add AS (

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b' THEN ("output_amountA"/1e18)
      WHEN "tokenB" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b' THEN ("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushiswap_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM sushi."Router02_call_addLiquidity"
  WHERE "tokenA" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
    OR "tokenB" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'

),

sushiswap_remove AS (


  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'sushiswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidity"
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
    'sushiswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityWithPermit"
  WHERE "tokenA" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
    OR "tokenB" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'

),


uniswapv3_add as (

SELECT
  "from" as address,
  amount0 / 1e18 as amount,
  date_trunc('day', block_time) AS evt_block_day,
  'uniswapv3_add' as type,
  hash as evt_tx_hash
  
  FROM uniswap_v3."Pair_evt_Mint" m
  LEFT JOIN ethereum."transactions" tx ON m.evt_tx_hash = tx.hash
  WHERE tx.block_time > '5/4/21'
  and contract_address = '\x87d1b1A3675fF4ff6101926C1cCE971cd2D513eF'
  
),


uniswapv3_remove as (

SELECT
  "from" as address,
  -amount0 / 1e18 as amount,
  date_trunc('day', block_time) AS evt_block_day,
  'uniswapv3_add' as type,
  hash as evt_tx_hash
  
  FROM uniswap_v3."Pair_evt_Burn" m
  LEFT JOIN ethereum."transactions" tx ON m.evt_tx_hash = tx.hash
  WHERE tx.block_time > '5/4/21'
  and contract_address = '\x87d1b1A3675fF4ff6101926C1cCE971cd2D513eF'
  
),



lp AS (

SELECT * FROM sushiswap_add

UNION ALL

SELECT * FROM sushiswap_remove

UNION ALL 

SELECT * FROM uniswapv3_add

UNION ALL 

SELECT * FROM uniswapv3_remove

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
      'sushiswap_add', 'sushiswap_remove', 'uniswapv3_add', 'uniswapv3_remove')
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
-- enter positions when any address is exposed to BTC2x-FLI for the first time 
enterpositiondates as (select address userenters, min(evt_block_day) userentersdate from temp
group by address),

--exit postions table (addresses may have gone to zero exposure multiple times)
exitpostiondates as (select address userexits,  max(evt_block_day) exitdate from temp
where exposure = 0
group by address), 

overall as (
select userenters, 
date_trunc('day', userentersdate) as userentersdate, 
coalesce(userexits,userenters),
coalesce(exitdate, date_trunc('day', current_timestamp) ) as exitdate 
from enterpositiondates
full outer join exitpostiondates on userenters = userexits), 
retention as (select *, (exitdate-userentersdate) as retainedfor from overall), 
lessthanamonth as (
select count(userenters) as users from retention
where retainedfor < '30 days'
), 
onetotwomonths as (
select count(userenters) as users from retention
where retainedfor < '60 days'
and retainedfor > '30 days'
),
twotothreemonths as (
select count(userenters) as users from retention
where retainedfor > '60 days'
and retainedfor < '90 days'
),
threetofourmonths as (
select count(userenters) as users from retention
where retainedfor > '90 days'
and retainedfor < '120 days'
), 
allretentions as (
select users, '0-1 months' as holdingtime from lessthanamonth
union all 
select users, '1-2 months' as holdingtime from onetotwomonths
union all 
select users, '2-3 months' as holdingtime from twotothreemonths
union all 
select users, '3-4 months' as holdingtime from threetofourmonths
)
select * from allretentions



