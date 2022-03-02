-- https://dune.xyz/queries/194587
with 
-- Provide symbol of focal token (the token for which address exposure is needed) as input
token as (
select 
            * 
from        erc20.tokens 
where       symbol = 'DPI'
)

-- Pull all uniswap v2 pools where focal token is `token0` or `token1`
, uniswapv2_pool as (
SELECT
            token0,
            erc20.decimals as decimals0,
            erc20.symbol as symbol0,
            token1,
            y.decimals as decimals1,
            y.symbol as symbol1,
            pair as pool
FROM        uniswap_v2."Factory_evt_PairCreated" pairsraw
INNER JOIN  token erc20 ON pairsraw.token0 = erc20.contract_address
INNER JOIN  erc20.tokens y ON pairsraw.token1 = y.contract_address
-- where       pair != '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'

union all

SELECT
            token0,
            x.decimals as decimals0,
            x.symbol as symbol0,
            token1,
            erc202.decimals as decimals1,
            erc202.symbol as symbol1,
            pair as pool
FROM        uniswap_v2."Factory_evt_PairCreated" pairsraw
INNER JOIN  token erc202 ON pairsraw.token1 = erc202.contract_address
INNER JOIN  erc20.tokens x ON pairsraw.token0 = x.contract_address
-- where       pair != '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
)

, uniswap_v2 as (

with 

-- Find all addresses that have LP'd into focal pools. Daily periodicity
uniswapv2_add as (

SELECT
        	"from" as address,
        	p.pool,
        	symbol0,
        	symbol1,
        	decimals0,
        	decimals1,
            date_trunc('day', block_time) AS evt_block_day,
            'uniswapv2_add' as type,
            sum(amount0 * amount1) as amount -- This is liquidity that the LP position represents. While individual amount of tokens and their prices will change (due to impermanet loss), this product will remain constant for a position. Mints and burns to the position can simply be summed.
	
FROM        uniswap_v2."Pair_evt_Mint" m

INNER JOIN  uniswapv2_pool p
on          p.pool = m.contract_address

LEFT JOIN   ethereum."transactions" tx 
ON          m.evt_tx_hash = tx.hash

GROUP BY    1,2,3,4,5,6,7,8
)

-- Find all instances of removing (aka burning) liquidity. Daily periodicity
, uniswapv2_remove as (

SELECT
        	"from" as address,
        	p.pool,
        	symbol0,
        	symbol1,
        	decimals0,
        	decimals1,
        	date_trunc('day', block_time) AS evt_block_day,
        	'uniswapv2_remove' as type,
        	sum(-amount0 * amount1) as amount
	
FROM        uniswap_v2."Pair_evt_Burn" m

INNER JOIN  uniswapv2_pool p
on          p.pool = m.contract_address

LEFT JOIN   ethereum."transactions" tx 
ON          m.evt_tx_hash = tx.hash

GROUP BY    1,2,3,4,5,6,7,8
)

-- Aggregate `amount` over mints and burns. Daily periodicity
, combined as (
select 
            address,
        	pool,
        	symbol0,
        	symbol1,
        	decimals0,
        	decimals1,
        	evt_block_day,
        	sum(amount) as amount

from        (
            select * from uniswapv2_add
            union all
            select * from uniswapv2_remove
            ) as a
            
group by    1,2,3,4,5,6,7
)

, adj_liq as (
select a.*, coalesce(lead_time_intermediate, date_trunc('minute', current_timestamp)) as lead_time -- If there is no next LP action in the data, then that means LP position is the same until today
from    (
        select *,
        sum(amount) 
            over(partition by address, pool order by evt_block_day) as new_amount, -- Cum sum of amount to track changes in position due to LP's actions (mints and burns)
        lead(evt_block_day) 
            over(partition by address, pool order by evt_block_day) as lead_time_intermediate -- To identify the timing of next LP action
        from combined
        )a
)

-- Get a series of dates between current LP action and next LP action
, adj_liq_series as (
select      a.*,
            generate_series(date_trunc('day', evt_block_day), date_trunc('day', lead_time) - '1 day'::interval, '1 day'::interval) as days

from        adj_liq a

where       new_amount > 0
)

-- Following queries are for generating a price feed from Uniswap v2
, price_feed as (
select      
            pool.pool,
            case
                when "amount0In" > 0 and "amount1Out" > 0 then "amount1Out"/"amount0In"
                else "amount1In"/"amount0Out"
            end as price,
            evt_block_time as time,
            date_trunc('day', evt_block_time) as day

from        uniswapv2_pool pool

inner join  uniswap_v2."Pair_evt_Swap" b
on          pool.pool = b.contract_address
)

, last_prices as (
select      a.*
from        (
            select
                        a.*,
                        rank() over(partition by pool, day order by time desc) as time_rank
            from        price_feed as a
            ) a
where       time_rank = 1
)

, lead_date as (
select      a.*,
            lead(day) over(partition by pool order by day) as lead_day
from        last_prices as a
)

, closing_prices as (
select
            a.*,
            generate_series(date_trunc('day', day), date_trunc('day', coalesce(lead_day, current_date)) - '1 day'::interval, '1 day'::interval) as days

from        lead_date as a
)

-- Join LP data with price data and calculate token amounts accounting for IL
, real_liq as (
select      address, a.pool, symbol0, symbol1, a.days,
            price * (10^(decimals1-decimals0)) as price,
            sqrt(new_amount/price)/(10^decimals0) as amount0_real,
            sqrt(new_amount*price)/(10^decimals1) as amount1_real
            
from        adj_liq_series a

inner join  closing_prices b
on          a.pool = b.pool
and         a.days = b.days
)

, add_exposure as (
select      
            a.*,
            case
                when symbol0 = b.symbol then amount0_real
                when symbol1 = b.symbol then amount1_real
            end as focal_token_amount

from        real_liq as a
cross join  token b
)

select 
            address
            ,symbol1 || '-' || symbol0 as pool_name
            ,'uniswap_v2' as usage
            ,days
            ,focal_token_amount
from        add_exposure
-- WHERE focal_token_amount > 0

)

, sushiv2_pool as (
SELECT
            token0,
            erc20.decimals as decimals0,
            erc20.symbol as symbol0,
            token1,
            y.decimals as decimals1,
            y.symbol as symbol1,
            pair as pool
FROM        sushi."Factory_evt_PairCreated" pairsraw
INNER JOIN  token erc20 ON pairsraw.token0 = erc20.contract_address
INNER JOIN  erc20.tokens y ON pairsraw.token1 = y.contract_address
-- where       pair != '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'

union all

SELECT
            token0,
            x.decimals as decimals0,
            x.symbol as symbol0,
            token1,
            erc202.decimals as decimals1,
            erc202.symbol as symbol1,
            pair as pool
FROM        sushi."Factory_evt_PairCreated" pairsraw
INNER JOIN  token erc202 ON pairsraw.token1 = erc202.contract_address
INNER JOIN  erc20.tokens x ON pairsraw.token0 = x.contract_address
-- where       pair != '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
)

, sushi_v2 as (

with 

-- Find all addresses that have LP'd into focal pools. Daily periodicity
sushiv2_add as (

SELECT
        	"from" as address,
        	p.pool,
        	symbol0,
        	symbol1,
        	decimals0,
        	decimals1,
            date_trunc('day', block_time) AS evt_block_day,
            'sushiv2_add' as type,
            sum(amount0 * amount1) as amount -- This is liquidity that the LP position represents. While individual amount of tokens and their prices will change (due to impermanet loss), this product will remain constant for a position. Mints and burns to the position can simply be summed.
	
FROM        sushi."Pair_evt_Mint" m

INNER JOIN  sushiv2_pool p
on          p.pool = m.contract_address

LEFT JOIN   ethereum."transactions" tx 
ON          m.evt_tx_hash = tx.hash

GROUP BY    1,2,3,4,5,6,7,8
)

-- Find all instances of removing (aka burning) liquidity. Daily periodicity
, sushiv2_remove as (

SELECT
        	"from" as address,
        	p.pool,
        	symbol0,
        	symbol1,
        	decimals0,
        	decimals1,
        	date_trunc('day', block_time) AS evt_block_day,
        	'sushiv2_remove' as type,
        	sum(-amount0 * amount1) as amount
	
FROM        sushi."Pair_evt_Burn" m

INNER JOIN  sushiv2_pool p
on          p.pool = m.contract_address

LEFT JOIN   ethereum."transactions" tx 
ON          m.evt_tx_hash = tx.hash

GROUP BY    1,2,3,4,5,6,7,8
)

-- Aggregate `amount` over mints and burns. Daily periodicity
, combined as (
select 
            address,
        	pool,
        	symbol0,
        	symbol1,
        	decimals0,
        	decimals1,
        	evt_block_day,
        	sum(amount) as amount

from        (
            select * from sushiv2_add
            union all
            select * from sushiv2_remove
            ) as a
            
group by    1,2,3,4,5,6,7
)

, adj_liq as (
select a.*, coalesce(lead_time_intermediate, date_trunc('minute', current_timestamp)) as lead_time -- If there is no next LP action in the data, then that means LP position is the same until today
from    (
        select *,
        sum(amount) 
            over(partition by address, pool order by evt_block_day) as new_amount, -- Cum sum of amount to track changes in position due to LP's actions (mints and burns)
        lead(evt_block_day) 
            over(partition by address, pool order by evt_block_day) as lead_time_intermediate -- To identify the timing of next LP action
        from combined
        )a
)

-- Get a series of dates between current LP action and next LP action
, adj_liq_series as (
select      a.*,
            generate_series(date_trunc('day', evt_block_day), date_trunc('day', lead_time) - '1 day'::interval, '1 day'::interval) as days

from        adj_liq a

where       new_amount > 0
)

-- Following queries are for generating a price feed from Uniswap v2
, price_feed as (
select      
            pool.pool,
            case
                when "amount0In" > 0 and "amount1Out" > 0 then "amount1Out"/"amount0In"
                else "amount1In"/"amount0Out"
            end as price,
            evt_block_time as time,
            date_trunc('day', evt_block_time) as day

from        sushiv2_pool pool

inner join  sushi."Pair_evt_Swap" b
on          pool.pool = b.contract_address
)

, last_prices as (
select      a.*
from        (
            select
                        a.*,
                        rank() over(partition by pool, day order by time desc) as time_rank
            from        price_feed as a
            ) a
where       time_rank = 1
)

, lead_date as (
select      a.*,
            lead(day) over(partition by pool order by day) as lead_day
from        last_prices as a
)

, closing_prices as (
select
            a.*,
            generate_series(date_trunc('day', day), date_trunc('day', coalesce(lead_day, current_date)) - '1 day'::interval, '1 day'::interval) as days

from        lead_date as a
)

-- Join LP data with price data and calculate token amounts accounting for IL
, real_liq as (
select      address, a.pool, symbol0, symbol1, a.days,
            price * (10^(decimals1-decimals0)) as price,
            sqrt(new_amount/price)/(10^decimals0) as amount0_real,
            sqrt(new_amount*price)/(10^decimals1) as amount1_real
            
from        adj_liq_series a

inner join  closing_prices b
on          a.pool = b.pool
and         a.days = b.days
)

, add_exposure as (
select      
            a.*,
            case
                when symbol0 = b.symbol then amount0_real
                when symbol1 = b.symbol then amount1_real
            end as focal_token_amount

from        real_liq as a
cross join  token b
)

select 
            address
            ,symbol1 || '-' || symbol0 as pool_name
            ,'sushi_v2' as usage
            ,days
            ,focal_token_amount
from        add_exposure
-- WHERE focal_token_amount > 0
)

, uniswapv3_pool as (
  select
              pool,
              token0,
              tok0.symbol as symbol0,
              tok0.decimals as decimals0,
              token1,
              tok1.symbol as symbol1,
              tok1.decimals as decimals1

  from        uniswap_v3."Factory_evt_PoolCreated" pool
  
  inner join  erc20."tokens" tok0 
  on          pool.token0 = tok0.contract_address
  
  inner join  erc20."tokens" tok1 
  on          pool.token1 = tok1.contract_address

  where       token0 = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
  or          token1 = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
)

, uniswap_v3 as (
with uniswapv3_add as (

SELECT
	"from" as address,
	p.pool,
	"tickUpper",
	"tickLower",
	CASE 
        WHEN "tickLower" < 0 THEN (1.0001^("tickLower")) * (10^(ABS(decimals0-decimals1)))
        ELSE (1.0001^("tickLower")) / (10^(ABS(decimals0-decimals1)))
    END AS price_lower,
    CASE 
        WHEN "tickUpper" < 0 THEN (1.0001^("tickUpper")) * (10^(ABS(decimals0-decimals1)))
        ELSE (1.0001^("tickUpper")) / (10^(ABS(decimals0-decimals1)))
    END AS price_upper,
	output_amount0 / (10^decimals0) as amount0,
	output_amount1 / (10^decimals1)  as amount1,
	amount,
	symbol0,
	symbol1,
	decimals0,
	decimals1,
  case
    when symbol0 = 'DPI' then output_amount0 / (10^decimals0) 
    when symbol1 = 'DPI' then output_amount1 / (10^decimals1) 
  end as amount_focal_token,
    date_trunc('minute', block_time) as block_time,
    date_trunc('day', block_time) AS evt_block_day,
    'uniswapv3_add' as type,
    hash as evt_tx_hash,
    call_block_number
	
	FROM uniswap_v3."Pair_call_mint" m
    
    INNER JOIN uniswapv3_pool p
    on p.pool = m.contract_address
	
	LEFT JOIN ethereum."transactions" tx 
	ON m.call_tx_hash = tx.hash
	
	WHERE tx.block_time > '5/4/21'
	and call_success is true
	
),

uniswapv3_remove as (

SELECT
	"from" as address,
	p.pool,
	"tickUpper",
	"tickLower",
	CASE
        WHEN "tickLower" < 0 THEN (1.0001^("tickLower")) * (10^(ABS(decimals0-decimals1)))
        ELSE (1.0001^("tickLower")) / (10^(ABS(decimals0-decimals1)))
    END AS price_lower,
    CASE 
        WHEN "tickUpper" < 0 THEN (1.0001^("tickUpper")) * (10^(ABS(decimals0-decimals1)))
        ELSE (1.0001^("tickUpper")) / (10^(ABS(decimals0-decimals1)))
    END AS price_upper,
	-output_amount0 / (10^decimals0) as amount0,
	-output_amount1 / (10^decimals1) as amount1,
	-amount as amount,
	symbol0,
	symbol1,
	decimals0,
	decimals1,
  case 
    when symbol0 = 'DPI' then -output_amount0 / (10^decimals0) 
    when symbol1 = 'DPI' then -output_amount1 / (10^decimals1) 
  end as amount_focal_token,
    date_trunc('minute', block_time) as block_time,
	date_trunc('day', block_time) AS evt_block_day,
	'uniswapv3_remove' as type,
	hash as evt_tx_hash,
    call_block_number
	
    FROM uniswap_v3."Pair_call_burn" m
    
    INNER JOIN uniswapv3_pool p
    on p.pool = m.contract_address
	
	LEFT JOIN ethereum."transactions" tx 
	ON m.call_tx_hash = tx.hash
	
	WHERE tx.block_time > '5/4/21'
	and call_success is true
),

combined as (
select * from uniswapv3_add
-- where address in ('\xd4ad5d62dce1a8bf661777a5c1df79bd12ac8f1d', '\xfee8e76c8d422921f76b0c10c47bb7ac43767eef', '\x2a99ec82d658f7a77ddebfd83d0f8f591769cb64')
union all
select * from uniswapv3_remove
-- where address in ('\xd4ad5d62dce1a8bf661777a5c1df79bd12ac8f1d', '\xfee8e76c8d422921f76b0c10c47bb7ac43767eef', '\x2a99ec82d658f7a77ddebfd83d0f8f591769cb64')
),

adj_liq as (
select a.*, coalesce(lead_time_intermediate, date_trunc('minute', current_timestamp)) as lead_time
from    (
        select *,
        sum(amount) 
            over(partition by address, pool, "tickUpper", "tickLower" order by block_time, call_block_number) as new_amount,
        lead(block_time) 
            over(partition by address, pool, "tickUpper", "tickLower" order by block_time) as lead_time_intermediate
        from combined
        )a
),

adj_liq_series as (
select      a.*,
            generate_series(date_trunc('day', block_time), date_trunc('day', lead_time) - '1 day'::interval, '1 day'::interval) as days
from        (
            select
                    *,
                    date_trunc('minute', block_time) as minutes,
                    rank() 
                        over(partition by address, pool, "tickUpper", "tickLower", evt_block_day order by block_time desc) as last_tx_of_day
            from    adj_liq
            ) a

where       last_tx_of_day = 1
and         new_amount > 0
),

price_feed as (
select      
            pool.pool,
            "sqrtPriceX96",
            ((power("sqrtPriceX96",2) * 10^(pool.decimals0 - pool.decimals1)) / (2^(96*2))) as price,
            evt_block_time as time,
            date_trunc('day', evt_block_time) as day

from        uniswapv3_pool pool

inner join  uniswap_v3."Pair_evt_Swap" b
on          pool.pool = b.contract_address
),

closing_prices as (
select      a.*
from        (
            select
                        a.*,
                        rank() over(partition by pool, day order by time desc) as time_rank
            from        price_feed as a
            ) a
where       time_rank = 1
),

liq_price as (
select      a.*,
            price,
            case
                when price between price_lower and price_upper then 'between'
                when price < price_lower then 'below'
                when price > price_upper then 'above'
            end as price_wrt_range,
            new_amount/sqrt(price_lower) as amount0_lp,
            new_amount*sqrt(price_lower) as amount1_lp,
            new_amount/sqrt(price_upper) as amount0_up,
            new_amount*sqrt(price_upper) as amount1_up,
            new_amount/sqrt(price) as amount0_cp,
            new_amount*sqrt(price) as amount1_cp
            
from        adj_liq_series a

inner join  closing_prices b
on          a.pool = b.pool
and         a.days = b.day
),

real_liq as (
select
            liq_price.*,
            case
                when price_wrt_range = 'between' then (amount0_cp - amount0_up)/10^decimals0
                when price_wrt_range = 'above' then 0
                when price_wrt_range = 'below' then (amount0_lp - amount0_up)/10^decimals0
            end as amount0_real,
            case
                when price_wrt_range = 'between' then (amount1_cp - amount1_lp)/10^decimals1
                when price_wrt_range = 'above' then (amount1_up - amount1_lp)/10^decimals1
                when price_wrt_range = 'below' then 0
            end as amount1_real

from        liq_price
),

add_exposure as (
select      
            a.*,
            case
                when symbol0 = 'DPI' then amount0_real
                when symbol1 = 'DPI' then amount1_real
            end as focal_token_amount

from        real_liq as a
)

select 
            address
            ,symbol1 || '-' || symbol0 as pool_name
            ,'uniswap_v3' as usage
            ,days
            ,focal_token_amount
from        add_exposure
-- WHERE focal_token_amount > 0
)

, cream AS (

    WITH cream_add AS (

        SELECT
          "minter" AS address,
          ("mintAmount"/1e18) AS amount,
          date_trunc('day', evt_block_time) AS evt_block_day,
          'cream_add' AS type,
          evt_tx_hash
        FROM creamfinance."CErc20Delegate_evt_Mint"
        WHERE contract_address = '\x2A537Fa9FFaea8C1A41D3C2B68a9cb791529366D'
    
    ),

    cream_remove AS (
    
        SELECT
          "redeemer" AS address,
          -("redeemAmount"/1e18) AS amount,
          date_trunc('day', evt_block_time) AS evt_block_day,
          'cream_remove' AS type,
          evt_tx_hash
        FROM creamfinance."CErc20Delegate_evt_Redeem"
        WHERE contract_address = '\x2A537Fa9FFaea8C1A41D3C2B68a9cb791529366D'
    
    ),
    
    cream_combined AS (
    
        SELECT * FROM cream_add
        
        UNION ALL
        
        SELECT * FROM cream_remove
    
    ),
    
    cream_temp AS (
    
        SELECT
            address,
            evt_block_day AS dt,
            SUM(amount) AS amount
        FROM cream_combined
        GROUP BY 1, 2
        HAVING SUM(amount) > 0
    
    ),
    
    cream_by_date  AS (

        SELECT
            DISTINCT
            t1.address,
            t2.dt
        FROM cream_temp t1
        CROSS JOIN (
            SELECT generate_series((SELECT date_trunc('day', MIN(evt_block_day)) FROM cream_add), date_trunc('day', NOW()), '1 day') AS dt
        ) t2
        
    ),
    
    temp AS (

      SELECT
        a.address,
        a.dt,
        CASE b.amount
            WHEN NULL THEN 0
            ELSE b.amount
        END AS exposure
      FROM cream_by_date a
      LEFT JOIN cream_temp b ON a.address = b.address AND a.dt = b.dt

    ),

    cream_address_over_time AS (
    
        SELECT
            address,
            dt,
            sum(exposure) OVER (PARTITION BY address ORDER BY DT ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exposure
        FROM temp
    
    )
    
    SELECT 
        address,
        'cream' AS pool_name,
        'cream' AS usage,
        dt AS days,
        exposure AS focal_token_amount
    FROM cream_address_over_time
    WHERE exposure IS NOT NULL
        -- AND exposure > 0

)

, contracts_to_remove as (
    select '\x0000000000000000000000000000000000000000' as address
    
    union all
    
    select distinct pool as address from uniswapv3_pool
    
    union all
    
    select distinct pool as address from uniswapv2_pool
    
    UNION ALL
    
    SELECT 'x2A537Fa9FFaea8C1A41D3C2B68a9cb791529366D' AS address 
)

, wallet AS (

    WITH wallet AS (
    
        SELECT
            DISTINCT ON (date_trunc('day', "timestamp"), wallet_address) date_trunc('day', "timestamp") AS dt,
            "timestamp",
            wallet_address,
            token_address,
            amount_raw / 10^18 AS amount
        FROM erc20."token_balances"
        WHERE token_address = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
            -- AND wallet_address = '\x0000000000007f150bd6f54c40a34d7c3d5e9f56'
        ORDER BY date_trunc('day', "timestamp"), wallet_address, "timestamp" DESC
        
    ),
    
    days AS (
    
        SELECT generate_series('2020-09-10'::timestamp, date_trunc('day', NOW()), '1 day') AS dt -- Generate all days
    
    ),
    
    address_date AS (
    
        SELECT
            DISTINCT
            t2.dt,
            t1.wallet_address
        FROM wallet t1
        CROSS JOIN (
            SELECT
                dt
            FROM days
        ) t2
        
    ),
    
    wallet_address_date AS (
    
        SELECT
            ad.dt,
            ad.wallet_address,
            w.amount
        FROM address_date ad
        LEFT JOIN wallet w ON ad.dt = w.dt AND ad.wallet_address = w.wallet_address
        ORDER BY 2, 1
    
    ),
    
    wallet_address_date_amount AS (
    
        SELECT
            dt,
            wallet_address AS address,
            (ARRAY_REMOVE(ARRAY_AGG(amount) OVER (PARTITION BY wallet_address ORDER BY dt), NULL))[COUNT(amount) OVER (PARTITION BY wallet_address ORDER BY dt)] AS exposure
        FROM wallet_address_date
        
    )
    
    SELECT
        *
    FROM wallet_address_date_amount
    WHERE exposure IS NOT NULL AND exposure > 0
    	AND address NOT IN (SELECT * FROM contracts_to_remove)

),

exposure AS (

    WITH combined AS (
    
        SELECT  address
                ,'spot' as pool_name
                ,'spot' as usage
                ,dt
                ,exposure
        FROM wallet
        
        UNION ALL 
        
        SELECT  address
                ,pool_name
                ,usage
                ,days as dt
                ,focal_token_amount as exposure
        FROM uniswap_v3
        
        UNION ALL 
        
        SELECT  address
                ,pool_name
                ,usage
                ,days as dt
                ,focal_token_amount as exposure
        FROM uniswap_v2
        
        UNION ALL 
        
        SELECT  address
                ,pool_name
                ,usage
                ,days as dt
                ,focal_token_amount as exposure
        FROM sushi_v2
        
        UNION ALL
        
        SELECT  address
                ,pool_name
                ,usage
                ,days as dt
                ,focal_token_amount as exposure
        FROM cream
    
    )
    
    SELECT
        dt
        ,address
        ,SUM(exposure) AS exposure
    FROM combined
    GROUP BY 1, 2

),

poap_addresses AS (

    SELECT * FROM (VALUES
        ('\x0100e4f2a841e773564c333f889ab6d6bd5fcb22'),
        ('\x05ba910d9b6128da5e2b9486d5edc00f8174f1c6'),
        ('\x05c9508aa6156f10bf9a7e43c080cc6eb3971e99'),
        ('\x06334ce2436d281359a74f8163186f4fa2e942f4'),
        ('\x08c6cab5bf0ae77833c5cbf8d2dd934a9253cd7b'),
        ('\x128dd6789a5ca88d46ea9861ac95d4a3d876539d'),
        ('\x1ef8019a1793ef3283742d7aa975b9a66133a2d0'),
        ('\x21bc7fbf89b07e318dce55676bff0c3ff5edb948'),
        ('\x29caeea733088cf59db91c673c66b50f390e8183'),
        ('\x2cdcf4ea746fe053737978e7e10807698f21e814'),
        ('\x2dbf79ad0ba63898b9fa414c35be03fb074972ba'),
        ('\x33bd61af24582c8742264d6a06f876fba211ef60'),
        ('\x35675299d9a0891da3597f9d1317ac2ca5c9c2ac'),
        ('\x358cef6068733f92b8c87e2dad0a03e0cc5f281c'),
        ('\x3f5b5d7d68164628c4b61ca3eccc4f877396f993'),
        ('\x41959aacd08402eca8f5c290a5643fc84d60eb7d'),
        ('\x421f223e19877d9765ecd6e8ec4812457229b36a'),
        ('\x428c210c2eb982a199d0f5d4cfcc4852c0519274'),
        ('\x480b2701a29a737b2f15f4876c392bd96a75d453'),
        ('\x49ccac5ed5715e34dacb3ae81022fa561efd9191'),
        ('\x4a35677c1b8450cd27b619ac7356a9eeeb0b4368'),
        ('\x4c3ada723a2d63eaf5d7225d41de1b96fe700a14'),
        ('\x4f8c2d5397262653cd8956cb977a0ba3660210c7'),
        ('\x53afc742f1de3be40f3693b3e37e6b0926dd2f91'),
        ('\x546c1528319f9fc621f5752d98690f28293a5c1d'),
        ('\x578152463e01de0fc1331250351dd6d11dafd9b3'),
        ('\x57f84c67bc0d85043858d9ebcc8f3d35bd336f5b'),
        ('\x5955f5b33e67571110ee1e40e540c072be63d094'),
        ('\x5eee4c61d5e63486dcd3eb4ad445403c9e1bb413'),
        ('\x635485da38e44eed324077760f17620e3d3991d4'),
        ('\x6626160e5e476a936d3d46f8bcb60414c0183410'),
        ('\x6b1050c1c6b288c79ac1db299dc481048abbbbcd'),
        ('\x7177494158c6a27b18f7aa485dd58f852c9fcaad'),
        ('\x7721f140c2968d5c639a9b40a1e6ca48a9b7c41d'),
        ('\x775936c4dca762d38e329930c60dc4a71b724ca1'),
        ('\x7b13e920e92688947819970d064c0a44afbf9b07'),
        ('\x7ee40e56c015832beb3c5b92dc8483e322ee5932'),
        ('\x7eec17130e51a067993f9b47d757987657b1ba6b'),
        ('\xd7e7bca98ab9fb25e17ad73429e89a40b55708be'),
        ('\x7fa0bbf4856bf37ea8d0e9d1b47514abf17beb84'),
        ('\xa6e59b844891e619801b298f4f0af52054513a3c'),
        ('\xe7fe2a9d8fa0f37b33f994dff5b4a3219fe343a2'),
        ('\x84511ca923bdb5f4b6ecf7a5b147f58767bf6c8e'),
        ('\x875a89c827b2c62688d6d4009c7c537799fd7fa2'),
        ('\x9b2829c0c4203e2ee0d9d61f94aa724271705a02'),
        ('\x9d7d2d5c305348faf3aa185d7114dcdd936d5b45'),
        ('\x9e49b413bf488202d21fbae112256509f41effd6'),
        ('\xa0e27626cb0f54a717ee3315b2a592e2bfbe7f48'),
        ('\xa6685809ad01cf447f81780e29529a331cdeadfe'),
        ('\xa86f7cb847bf41b049002cb0c96e6156a8b27e25'),
        ('\xae447efdb02d15d460d9c640710244f3ebad5473'),
        ('\xb462521ead6822caacec001daac0978d057dd611'),
        ('\xb7b01d03a6cdcb85a41368e0cd94f4dac1418536'),
        ('\xb8cf2227a96bd9f32e7a138733cf891c2f89ed17'),
        ('\xbea6c46af03c4ef9c9d96c810c17169651b1ed60'),
        ('\xc42705a210f082ff29e6beac80f56c41f0a54091'),
        ('\xd5513ddb9780e610c56ae32a29dafc3c7abc0a8c'),
        ('\xd5835db0959569622681dcff1e72b0936170bd6b'),
        ('\xd76ff76ac8019c99237bde08d7c39dab5481bed2'),
        ('\xd8895c04cb7d43ed16d83268d91cf946fffa4254'),
        ('\xd8d7c1148fc72638532e8b4ff3f1934fb7a08ed6'),
        ('\xda3372784273811144284e92fafe7f5dc6e4aa3d'),
        ('\xdad3fd6c9fb0c2b56228e58ae191b62bfb1bec83'),
        ('\xdb26aa474fc303b6757b327dd51a73cb8ae97987'),
        ('\xdb52f6a6c1f6d4f53b91f9ec6653e0dcc7bdbc00'),
        ('\xdd787ddbc72ea62e7c644b6206a23c05a5f9d487'),
        ('\xde5f94136b342f0f662f4be09b67dbb3ebf8f2dc'),
        ('\xf29dbc79ae222677f8a91dbe55cf052e61c206fd'),
        ('\xf6b7af3eaed85198ad9c49353485d17b54988e7a'),
        ('\xfe760978a84b58259a4c851b42bd8c9c7ef93e80'),
        ('\xff931c9f1ccaa5e6db3e52c52b64d39c842c5daa'),
        ('\xe9af62eb3dc36f4a39b52d2b3e06f09295bb1680'),
        ('\x9f2880427f86a15daedc4c6f3185e8affe2ba761'),
        ('\x57efcc7607cd2da49d73e8f9c88ed114cbdc5cf7'),
        ('\xb63a90e0dcfc1ee94c7b2dd827b1c7f68dfbac89'),
        ('\x2b384212edc04ae8bb41738d05ba20e33277bf33'),
        ('\x6a19891e91a3d4d690cc8f9627290d25bfd34df6'),
        ('\x1c956943024dd561ad820a75ac374922d21dbcda'),
        ('\x47b64ae719e7ce8bfef1627c4c58c5477792fb60'),
        ('\xe64b1a000b931080a73546d4ed5d742495ce2a92'),
        ('\x20b6fa95a915ec3ce053fa1d3a5759cb9118137a'),
        ('\x95ace38b839597bcbdc5d357776b18cb5d501cf5'),
        ('\x53fd9fe0837a281d02f91b61fd7ce2f7b60566bd'),
        ('\xb01474b50382fae1a847e3a916ecdf07ba57bcc7'),
        ('\xd338f79ce0615f7decba58fa3b71f215758b5406'),
        ('\x5f2091da87586684d69ce4d6f8e1897a0ac588eb'),
        ('\x2e8abfe042886e4938201101a63730d04f160a82'),
        ('\x32511f960f4b380cdde599065019094b309c2ce8'),
        ('\x7a74495d0f52683d5c0b04f804ed0b5efa083bd0'),
        ('\xe7708176a1464f34372bb35366e347811b6b5a26')
            ) AS t (address)
            
),

temp AS (

    SELECT
        *
    FROM exposure
    WHERE exposure > 0
        AND dt < date_trunc('day', NOW())
        AND address IN (
           SELECT address::bytea FROM poap_addresses
        )

),

days AS (
    
    SELECT generate_series('2020-09-10 00:00:00'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

address_by_date  AS (

    SELECT
        DISTINCT
        t1.address,
        t2.day
    FROM temp t1
    CROSS JOIN (
        SELECT
            DISTINCT(day)
        FROM days
    ) t2

)

-- SELECT * FROM exposure

-- -- SELECT
-- --     *
-- -- FROM temp
-- -- WHERE address = '\x0100e4f2a841e773564c333f889ab6d6bd5fcb22'

-- SELECT 
--     a.address,
--     a.day,
--     COALESCE(t.exposure, 0) AS exposure
-- FROM address_by_date a
-- LEFT JOIN temp t ON a.address = t.address AND a.day = t.dt
-- ORDER BY 1, 2

SELECT
CASE WHEN dt < '7-15-2021' THEN 'pre' ELSE 'post' AS period,
    AVG(exposure) AS avg_daily_exposure
FROM (
    SELECT 
        dt,
        SUM(exposure) AS exposure
    FROM temp
    GROUP BY 1
) 
