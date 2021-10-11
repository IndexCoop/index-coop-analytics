-- https://duneanalytics.com/queries/25253

with 
-- Provide symbol of focal token (the token for which address exposure is needed) as input
token as (
select 
            * 
from        erc20.tokens 
where       symbol = 'ETH2x-FLI'
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

  where       token0 = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
  or          token1 = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
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
    when symbol0 = 'ETH2x-FLI' then output_amount0 / (10^decimals0) 
    when symbol1 = 'ETH2x-FLI' then output_amount1 / (10^decimals1) 
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
	
	WHERE tx.block_time > '3-14-2021'
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
    when symbol0 = 'ETH2x-FLI' then -output_amount0 / (10^decimals0) 
    when symbol1 = 'ETH2x-FLI' then -output_amount1 / (10^decimals1) 
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
	
	WHERE tx.block_time > '3-14-21'
	and call_success is true
),

combined as (
select * from uniswapv3_add

union all
select * from uniswapv3_remove

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
                when symbol0 = 'ETH2x-FLI' then amount0_real
                when symbol1 = 'ETH2x-FLI' then amount1_real
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
)

, contracts_to_remove as (
select '\x0000000000000000000000000000000000000000' as address

union all

select distinct pool as address from uniswapv3_pool

union all

select distinct pool as address from uniswapv2_pool
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
        WHERE token_address = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
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

    )
    
    SELECT
        address
        ,dt
        ,SUM(exposure) AS exposure
    FROM combined
    GROUP BY 1, 2

)

SELECT
    dt,
    COUNT(DISTINCT(address))
FROM exposure
WHERE exposure > 0 AND dt < date_trunc('day', NOW())
GROUP BY 1

