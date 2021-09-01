-- https://duneanalytics.com/queries/89094

WITH uniswapv3_pool as (
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
    
      where       token0 = '\x2af1df3ab0ab157e1e2ad8f88a7d04fbea0c7dc6'
      or          token1 = '\x2af1df3ab0ab157e1e2ad8f88a7d04fbea0c7dc6'
    ),

uniswap_v3 AS (

    WITH     
    uniswapv3_add as (
    
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
        when symbol0 = 'BED' then output_amount0 / (10^decimals0) 
        when symbol1 = 'BED' then output_amount1 / (10^decimals1) 
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
        when symbol0 = 'BED' then -output_amount0 / (10^decimals0) 
        when symbol1 = 'BED' then -output_amount1 / (10^decimals1) 
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
                    when symbol0 = 'BED' then amount0_real
                    when symbol1 = 'BED' then amount1_real
                end as focal_token_amount
    
    from        real_liq as a
    )
    
    select
        address,
        days AS dt,
        SUM(focal_token_amount) AS exposure
    from add_exposure
    GROUP BY 1, 2

),

contracts_to_remove as (
select '\x0000000000000000000000000000000000000000' as address

union all

select distinct pool as address from uniswapv3_pool
),

wallet AS (

    WITH transfers AS (
    
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
    
    daily_transfers as (
	  SELECT
        address,
        evt_block_day AS dt,
        SUM(amount) AS amount
      FROM transfers
      GROUP BY 1, 2
	),
	
	address_by_date  AS (

        SELECT
            DISTINCT
            t1.address,
            t2.dt
        FROM daily_transfers t1
        CROSS JOIN (
            SELECT
                DISTINCT(dt)
            FROM daily_transfers
        ) t2
    
    ),
    
    temp AS (
    
      SELECT
        a.address,
        a.dt,
        CASE e.amount
            WHEN NULL THEN 0
            ELSE e.amount
        END AS exposure
      FROM address_by_date a
      LEFT JOIN daily_transfers e ON a.address = e.address AND a.dt = e.dt
    
    ),
    
    address_over_time AS (
    
        SELECT
            address,
            dt,
            sum(exposure) OVER (PARTITION BY address ORDER BY dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exposure
        FROM temp
    
    )
	
	SELECT * FROM address_over_time
	WHERE address NOT IN (SELECT * FROM contracts_to_remove)

),

exposure AS (

    WITH combined AS (
    
        SELECT address, dt, exposure
        FROM wallet
        
        UNION ALL 
        
        SELECT address, dt, exposure
        FROM uniswap_v3
    
    )
    
    SELECT
        address,
        dt,
        SUM(exposure) AS exposure
    FROM combined
    GROUP BY 1, 2

)

SELECT
    dt,
    COUNT(DISTINCT(address))
FROM exposure
WHERE exposure > 0
GROUP BY 1