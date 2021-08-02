--https://duneanalytics.com/queries/91206

with weth_pairs_v2 AS 
( -- Get exchange contract address and "other token" for WETH
    SELECT cr."pair" AS contract, 
        CASE WHEN cr."token0" = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' then '0' ELSE '1' END  AS eth_token,
        CASE WHEN cr."token1" = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' then cr."token0" ELSE cr."token1" END  AS other_token 
    FROM uniswap_v2."Factory_evt_PairCreated" cr
    WHERE token0 = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' OR  token1 = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
)
,
/*get swap data from uniswap v2*/
swap_v2 AS 
( 
    SELECT
        CASE WHEN eth_token = '0' then sw."amount0In" + sw."amount0Out" ELSE sw."amount1In" + sw."amount1Out"
        END/1e18 AS eth_amt, 
        CASE WHEN eth_token = '1' then sw."amount0In" + sw."amount0Out" ELSE sw."amount1In" + sw."amount1Out"
        END/power(10, tok."decimals") AS other_amt, -- If the token is not in the erc20.tokens list you can manually divide by 10^decimals
        tok."symbol",
        tok."contract_address",
        sw."evt_tx_hash",
        date_trunc('hour', sw."evt_block_time") AS hour
    FROM uniswap_v2."Pair_evt_Swap" sw
    JOIN weth_pairs_v2 ON sw."contract_address" = weth_pairs_v2."contract"
    JOIN erc20."tokens" tok ON weth_pairs_v2."other_token" = tok."contract_address"
    WHERE other_token in (
        '\x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9',
        '\x1f9840a85d5af5bf1d1762f925bdaddc4201f984',
        '\x0954906da0Bf32d5479e25f46056d22f08464cab',
        '\x72e364f2abdc788b7e918bc238b21f109cd634d7',
        '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd',
        '\xf629cbd94d3791c9250152bd8dfbdf380e2a3b9c',
        '\xbb0e17ef65f82ab018d8edd776e8dd940327b28b',
        '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b',
        '\xdac17f958d2ee523a2206206994597c13d831ec7',
        '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
        '\x6b175474e89094c44da98b954eedeac495271d0f'
       )
    AND sw.evt_block_time >= date_trunc('days',now())
),

/*pair info from uniswap v3*/
weth_pairs_v3 AS 
( -- Get exchange contract address and "other token" for WETH
    SELECT cr."pool" AS contract, 
        CASE WHEN cr."token0" = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' then '0' ELSE '1' END  AS eth_token,
        CASE WHEN cr."token1" = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' then cr."token0" ELSE cr."token1" END  AS other_token 
    FROM uniswap_v3."Factory_evt_PoolCreated" cr
    WHERE token0 = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' OR  token1 = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),

/*get swap data from uniswap v3*/
swap_v3 AS 
( 
    SELECT
        CASE WHEN eth_token = '0' then abs(sw."amount0") ELSE abs(sw."amount1") 
        END/1e18 AS eth_amt, 
        CASE WHEN eth_token = '1' then abs(sw."amount0") ELSE abs(sw."amount1")
        END/power(10, tok."decimals") AS other_amt, -- If the token is not in the erc20.tokens list you can manually divide by 10^decimals
        tok."symbol",
        tok."contract_address",
		sw."evt_tx_hash",
        date_trunc('hour', sw."evt_block_time") AS hour
    FROM uniswap_v3."Pair_evt_Swap" sw
    JOIN weth_pairs_v3 ON sw."contract_address" = weth_pairs_v3."contract"
    JOIN erc20."tokens" tok ON weth_pairs_v3."other_token" = tok."contract_address"
    WHERE other_token in (
        '\x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9',
        '\x1f9840a85d5af5bf1d1762f925bdaddc4201f984',
        '\x0954906da0Bf32d5479e25f46056d22f08464cab',
        '\x72e364f2abdc788b7e918bc238b21f109cd634d7',
        '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd',
        '\xf629cbd94d3791c9250152bd8dfbdf380e2a3b9c',
        '\xbb0e17ef65f82ab018d8edd776e8dd940327b28b',
        '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b',
        '\xdac17f958d2ee523a2206206994597c13d831ec7',
        '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
        '\x6b175474e89094c44da98b954eedeac495271d0f'
)
    AND sw.evt_block_time >= date_trunc('days',now())
),

/*union swap data of v2 and v3*/
raw_price_1 as
(
    select 
        x.contract_address as asset,
		x.symbol,
        sum(x.eth_amt)/sum(x.other_amt) as price_in_eth
    from (
	      select * from swap_v2
		  union all
		  select * from swap_v3
	     ) x
    group by x.contract_address,x.symbol
    
)
,
raw_price_2 as
(
    select
        c.asset,
		c.symbol,
        c.price_in_eth* a.price as price
    from (select 
              avg(price) as price	 		  
		  from prices.usd 
		  where symbol='WETH' 
		  and "minute" >= date_trunc('days',now())
			  
		  ) a	
    cross join  raw_price_1 c

),
--select  * from raw_price_2;

total_value_in_usd as 
(
    select
        holder,
        sum(amount*price)::int8 as total_value_usd
    from dune_user_generated.loren_token_balance a
    left join raw_price_2 b
    on a.tokens = b.symbol
    group by holder
)

select
    a.holder,
    total_value_usd,
    "DPI",
    "UNI",
    "AAVE", 
    "AXS", 
    "ENJ", 
    "ETH2x-FLI", 
    "INDEX", 
    "MVI", 
    "USDT",
    "USDC",
    "DAI",
    label 
from dune_user_generated.loren_250plus_dpi_balance a 
left join total_value_in_usd b
on a.holder = b.holder
order by "DPI" desc;




