-- https://duneanalytics.com/queries/64106

-- v2 vs v3 Price Impact (ETH:USDC)

WITH v2 AS (

with params as (
select
            6 as dec0
            ,18 as dec1
            , '\xb4e16d0168e52d35cacd2c6185b44281ec28c9dc'::bytea as pool
) 

, swap as (
select      
            evt_block_time as swap_time
            ,evt_tx_hash as swap_tx
            ,"amount0In"/10^p.dec0 as amount0in
            ,"amount1In"/10^p.dec1 as amount1in
            ,"amount0Out"/10^p.dec0 as amount0out
            ,"amount1Out"/10^p.dec1 as amount1out
            ,concat(evt_block_number, coalesce(lpad(evt_index::text,3,'0'), '00')) as ky --key to join on so order of transactions is known even inside the block

from        uniswap_v2."Pair_evt_Swap" swap
inner join  params p
on          p.pool = swap.contract_address

where       date_trunc('day', evt_block_time) > '2021-04-01' -- for testing
)

, sync as (
select      
            evt_block_time as sync_time
            ,evt_tx_hash as sync_tx
            ,reserve0/10^p.dec0 as reserve0
            ,reserve1/10^p.dec1 as reserve1
            ,concat(evt_block_number, coalesce(lpad(evt_index::text,3,'0'), '00')) as ky --key to join on so order of transactions is known even inside the block

from        uniswap_v2."Pair_evt_Sync" sync
inner join  params p
on          p.pool = sync.contract_address

where       date_trunc('day', evt_block_time) > '2021-04-01' -- for testing
)


-- objective of sync_swap CTE: get the last synced reserves right before the swap
, sync_swap as (
select      *

from        (
            select      a.*
                        ,rank() over(partition by ky order by order_diff asc) as order_rnk
            
            from        (
                        select      a.*
                                    ,b.sync_time
                                    ,b.ky as bky
                                    ,cast(a.ky as decimal(12,0)) - cast(b.ky as decimal(12,0)) as order_diff
                                    ,b.reserve0
                                    ,b.reserve1
                                    ,b.reserve0 - a.amount0in as pre_swap_reserve0
                                    ,b.reserve1 + amount1out  as pre_swap_reserve1
                                    ,(b.reserve1 + amount1out)/(b.reserve0 - a.amount0in) as price_pre_swap
                        
                        from        swap a 
                        inner join  sync b
                        on          a.ky > b.ky 
                        and         sync_tx = swap_tx
                        ) a
            ) a

where       1=1
and         order_rnk = 1
and         amount0in > 0
)

, price_impact as (

select
            swap_time
            ,sync_time
            ,amount0in + amount0out AS amount0_actual
            ,amount1out + amount1in AS amount1_actual
            ,((amount1out + amount1in)/(amount0in + amount0out)) as swap_price
            ,price_pre_swap
            ,((((amount1out + amount1in)/(amount0in + amount0out)) - price_pre_swap)/price_pre_swap) * 100 as price_impact_percentage
            
from        sync_swap

where       abs(amount1out + amount1in) > 50 -- Buys of ETH

)

select
    
            swap_time AS time
            ,'v2' AS version
            ,abs(amount1_actual) AS eth
            ,abs(price_impact_percentage) AS price_impact
            

from price_impact

),

v3 AS (

with params as (
select
            6 as dec0
            ,18 as dec1
            , '\x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'::bytea as pool
)

, price_impact as (
select      *
            ,((price_paid - lag(post_swap_price) 
                over(order by evt_block_number asc))/lag(post_swap_price) 
                over(order by evt_block_number asc)) * 100 as price_impact_percentage
            ,lag(post_swap_price) 
                over(order by evt_block_number asc) as pre_swap_price

from        (
            select 
                        evt_block_time
                        ,evt_block_number
                        ,amount0/10^p.dec0 as amount0_actual
                        ,amount1/10^p.dec1 as amount1_actual
                        ,abs(amount1/10^p.dec1)/abs(amount0/10^p.dec0) as price_paid --quantity_y/quantity_x is the price paid for the swap
                        ,((power("sqrtPriceX96",2) * 10^(p.dec0-p.dec1)) / (2^(96*2))) as post_swap_price -- As per docs, sqrtPriceX96 records the sqrt(price) after the swap
                        -- , lag(((power("sqrtPriceX96",2) * 10^(p.dec0-p.dec1)) / (2^(96*2)))) 
                        --     over(partition by contract_address order by evt_block_number asc) as pre_swap_price -- This is simply a lag of post swap price. The post swap price of previous trade is the pre swap price of current trade
            
            
            from        uniswap_v3."Pair_evt_Swap" swap
            inner join  params p
            on          p.pool = swap.contract_address
            ) a
            

where       abs(amount1_actual) > 50 

)

select
            
            evt_block_time AS time
            ,'v3' AS version
            ,abs(amount1_actual) AS eth
            ,abs(price_impact_percentage) AS price_impact

from price_impact

),

trades AS (

    SELECT * FROM v2
    
    UNION ALL
    
    SELECT * FROM v3

)

SELECT * FROM trades