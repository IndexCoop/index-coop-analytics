WITH token as (

    SELECT
        contract_address,
        decimals,
        symbol
    FROM erc20.tokens 
    WHERE contract_address = '\x33d63ba1e57e54779f7ddaeaa7109349344cf5f1'

    UNION DISTINCT
    
    SELECT
        '\x33d63ba1e57e54779f7ddaeaa7109349344cf5f1'::bytea AS contract_address,
        18 AS decimals,
        'DATA' AS symbol
    
),

sushi_v2_pools AS (

    SELECT
        token0,
        erc20.decimals as decimals0,
        erc20.symbol as symbol0,
        token1,
        y.decimals as decimals1,
        y.symbol as symbol1,
        pair as pool
    FROM sushi."Factory_evt_PairCreated" pairsraw
    INNER JOIN token erc20 ON pairsraw.token0 = erc20.contract_address
    INNER JOIN erc20.tokens y ON pairsraw.token1 = y.contract_address
    
    UNION ALL
    
    SELECT
        token0,
        x.decimals as decimals0,
        x.symbol as symbol0,
        token1,
        erc202.decimals as decimals1,
        erc202.symbol as symbol1,
        pair as pool
    FROM sushi."Factory_evt_PairCreated" pairsraw
    INNER JOIN token erc202 ON pairsraw.token1 = erc202.contract_address
    INNER JOIN erc20.tokens x ON pairsraw.token0 = x.contract_address

),

sushi_v2 AS (
    
    WITH sushiv2_add as (
    -- Find all addresses that have LP'd into focal pools. Daily periodicity
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
        FROM sushi."Pair_evt_Mint" m
        INNER JOIN  sushi_v2_pools p ON p.pool = m.contract_address
        LEFT JOIN   ethereum."transactions" tx ON m.evt_tx_hash = tx.hash
        GROUP BY    1,2,3,4,5,6,7,8
    
    ),
    
    sushiv2_remove as (
    -- Find all instances of removing (aka burning) liquidity. Daily periodicity
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
        FROM sushi."Pair_evt_Burn" m
        INNER JOIN sushi_v2_pools p ON p.pool = m.contract_address
        LEFT JOIN   ethereum."transactions" tx ON m.evt_tx_hash = tx.hash
        GROUP BY    1,2,3,4,5,6,7,8
    
    ),
    
    combined as (
    -- Aggregate `amount` over mints and burns. Daily periodicity
        SELECT
            address,
        	pool,
        	symbol0,
        	symbol1,
        	decimals0,
        	decimals1,
        	evt_block_day,
        	sum(amount) as amount
        FROM (
            SELECT * FROM sushiv2_add
            UNION ALL
            SELECT * FROM sushiv2_remove
            ) AS a
        GROUP BY 1,2,3,4,5,6,7
    
    ),
    
    adj_liq as (
    
        SELECT 
            a.*, 
            coalesce(lead_time_intermediate, date_trunc('minute', current_timestamp)) as lead_time -- If there is no next LP action in the data, then that means LP position is the same until today
        FROM (
            SELECT 
                *,
                sum(amount) 
                    over(partition by address, pool order by evt_block_day) as new_amount, -- Cum sum of amount to track changes in position due to LP's actions (mints and burns)
                lead(evt_block_day) 
                    over(partition by address, pool order by evt_block_day) as lead_time_intermediate -- To identify the timing of next LP action
            FROM combined
            ) AS a
    
    ),
    
    adj_liq_series as (
    -- Get a series of dates between current LP action and next LP action
        SELECT
            a.*,
            generate_series(date_trunc('day', evt_block_day), date_trunc('day', lead_time) - '1 day'::interval, '1 day'::interval) as days
        FROM adj_liq a
        WHERE new_amount > 0
        
    ),
    
    price_feed as (
    -- Following queries are for generating a price feed from Uniswap v2
        WITH swaps AS (
    
            SELECT      
                pool.pool,
                case
                    when "amount0In" > 0 and "amount1Out" > 0 then "amount1Out"/"amount0In"
                    else "amount1In"/"amount0Out"
                end as price,
                evt_block_time as time,
                date_trunc('day', evt_block_time) as day
            FROM sushi_v2_pools pool
            INNER JOIN sushi."Pair_evt_Swap" b
            ON pool.pool = b.contract_address
        
        ),
    
        last_prices AS (
        
            SELECT
                a.*
            FROM (
                SELECT
                    a.*,
                    rank() over(partition by pool, day order by time desc) as time_rank
                FROM swaps as a
                ) a
            WHERE time_rank = 1
        
        ),
        
        lead_date AS (
        
            SELECT
                a.*,
                lead(day) over(partition by pool order by day) AS lead_day
            FROM last_prices AS a
        
        ),
        
        closing_prices AS (
        
            SELECT
                a.*,
                generate_series(date_trunc('day', day), date_trunc('day', coalesce(lead_day, current_date)) - '1 day'::interval, '1 day'::interval) as days
            FROM lead_date AS a
            
        )
    
        SELECT
            *
        FROM closing_prices
    
    ),
    
    real_liq AS (
    -- Join LP data with price data and calculate token amounts accounting for IL
        select
            address, 
            a.pool, 
            symbol0, 
            symbol1, 
            a.days,
            price * (10^(decimals1-decimals0)) as price,
            sqrt(new_amount/price)/(10^decimals0) as amount0_real,
            sqrt(new_amount*price)/(10^decimals1) as amount1_real
        FROM adj_liq_series a
        INNER JOIN price_feed b ON a.pool = b.pool AND a.days = b.days
        
    ),
    
    add_exposure AS (
    
        SELECT      
            a.*,
            CASE
                WHEN symbol0 = b.symbol THEN amount0_real
                WHEN symbol1 = b.symbol THEN amount1_real
            END AS focal_token_amount
        FROM real_liq AS a
        CROSS JOIN token b
    
    )
    
    SELECT
        address,
        symbol1 || '-' || symbol0 AS pool_name,
        'sushi_v2' AS usage,
        days,
        focal_token_amount
    FROM add_exposure

),

contracts_to_remove AS (
    
    SELECT '\x0000000000000000000000000000000000000000' AS address
    
    UNION ALL
    
    SELECT distinct pool AS address FROM sushi_v2_pools
    
),

wallet AS (

    WITH wallet AS (
    
        SELECT
            DISTINCT ON (date_trunc('day', "timestamp"), wallet_address) date_trunc('day', "timestamp") AS dt,
            "timestamp",
            wallet_address,
            token_address,
            amount_raw / 10^18 AS amount
        FROM erc20."token_balances"
        WHERE token_address = '\x33d63ba1e57e54779f7ddaeaa7109349344cf5f1'
        ORDER BY date_trunc('day', "timestamp"), wallet_address, "timestamp" DESC
        
    ),
    
    days AS (
    
        SELECT generate_series('2021-09-23'::timestamp, date_trunc('day', NOW()), '1 day') AS dt -- Generate all days
    
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
                ,'wallet' as pool_name
                ,'wallet' as usage
                ,dt
                ,exposure
        FROM wallet
        
        UNION ALL 
        
        SELECT  address
                ,pool_name
                ,usage
                ,days as dt
                ,focal_token_amount as exposure
        FROM sushi_v2
    
    )
    
    SELECT
        address
        ,pool_name
        ,usage
        ,dt
        ,SUM(exposure) AS exposure
    FROM combined
    GROUP BY 1, 2, 3, 4

)

SELECT
    dt,
    COUNT(DISTINCT(address))
FROM exposure
WHERE exposure > 0 AND dt < date_trunc('day', NOW()) AND dt >= '9-23-2021'
GROUP BY 1