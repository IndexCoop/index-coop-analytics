/* dune query here: https://duneanalytics.com/queries/67918
This query compares all time MVI NAV with it's market price by hour.
How it works?
1. Most of the tokens in mvi index have no price data on Dune, so we must build a price feed by using uniswap's trading data.
2.figures out the composition of per MVI each hour,then times the price of each composition at that time,adds them up gets the mvi nav
3.compares mvi nav with mvi market price 
*/

with start_date_of_past_days as
(
    select
        min(hour_date) as start_date
    from (
           select
                date_trunc('hour', "evt_block_time") as hour_date
           from setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
           where "_setToken" IN ('\x72e364f2abdc788b7e918bc238b21f109cd634d7')
         ) x
)
,

index_mint_amount as
(
    select
        *
    from (
          select
              "evt_tx_hash",
              "evt_block_time",
              date_trunc('hour', "evt_block_time") as hour_date,
              "_quantity"/1e18 as mint_amount,
              row_number() over(partition by date_trunc('hour', "evt_block_time") order by  "evt_block_time" desc) as rnb
          from setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
          where "_setToken" IN ('\x72e364f2abdc788b7e918bc238b21f109cd634d7')

         ) x 
    where x.rnb = 1
)
,
/*index composition when minted*/
index_composition as
(
    select
        a."evt_tx_hash",
        a."evt_block_time",
        date_trunc('hour', a."evt_block_time") as hour_date,
        a."contract_address" as asset_address,
        a."value"/10^b."decimals"/c.mint_amount as asset_amount,
        c.mint_amount,
        b.symbol
    from (
          select
              *
          from erc20."ERC20_evt_Transfer"
          where "evt_tx_hash" in (select "evt_tx_hash" from index_mint_amount )
          and "to" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
          and  "contract_address" != '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
         )  a
    left join erc20."tokens" b
    on a."contract_address" = b."contract_address"
    left join index_mint_amount c
    on a."evt_tx_hash" = c."evt_tx_hash"
    
)
,
/*composition in and out of index time*/
index_composition_in_out_time as
(
    select
        asset_address,
        in_index_time,
        case when last_time < last_mint_time then last_time else now() end as out_index_time
    from (
          select
             asset_address,
             min(hour_date) as in_index_time,
             max(hour_date) as last_time,
             (select max(hour_date) from index_composition ) as last_mint_time
          from index_composition 
          group by asset_address
          ) x 

)
,

index_composition_time_series as
(
    select
        date_trunc('hour', x) as price_time
    from generate_series( (select min(hour_date) from index_composition),
                          now(),
                          interval  '1 hour'
                         ) t(x)
)
,
/*index composition by hour*/
index_composition_by_hour as
(
    select
        contract_address,
        price_time,
        asset_amount,
        first_value(asset_amount) over (partition by contract_address,grp_asset order by price_time ) as asset_correct
    from (
          select
              a.price_time,
              x.contract_address,
              b.asset_amount,
              sum(case when b.asset_amount is not null then 1 end) over (partition by x.contract_address order by a.price_time) as grp_asset
          from index_composition_time_series a
          cross join (select distinct asset_address as contract_address from index_composition ) x
          left join index_composition b
          on a.price_time = b.hour_date 
            and b.asset_address = x.contract_address
         ) x
)
,
start_time_table as
(
    select 
        (select start_date from start_date_of_past_days) as start_time,
    	(select start_date from start_date_of_past_days) - interval '3 days' as start_time_1
	

),

/*use uniswap v2 and v3 as price feed*/

/*get mvi index compositions*/
index_tokens as 
(
    select 
	    distinct asset_address,symbol  
	from index_composition
	union 
	select '\x72e364f2abdc788b7e918bc238b21f109cd634d7' as asset_address,'MVI' as symbol

),
/*pair info from uniswap v2*/
weth_pairs_v2 AS 
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
    WHERE other_token in (select asset_address from index_tokens)
    AND sw.evt_block_time >= (select start_time_1 from start_time_table)
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
    WHERE other_token in (select asset_address from index_tokens)
    AND sw.evt_block_time >= (select start_time_1 from start_time_table)
),

/*union swap data of v2 and v3*/
raw_price_1 as
(
    select 
        x.hour as block_time,
        x.contract_address as asset,
		x.symbol,
        sum(x.eth_amt)/sum(x.other_amt) as price_in_eth
    from (
	      select * from swap_v2
		  union all
		  select * from swap_v3
	     ) x
    group by x.hour,x.contract_address,x.symbol
    
)
,
raw_price_2 as
(
    select
        a."hour" as block_time,
        b.asset,
		b.symbol,
        c.price_in_eth* a.price as price
    from (select 
	          date_trunc('hour', "minute") as "hour",
              avg(price) as price	 		  
		  from prices.usd 
		  where symbol='WETH' 
		  and "minute" >= (select start_time_1 from start_time_table)
		  group by date_trunc('hour', "minute")
			  
		  ) a	
    cross join (select asset_address as asset,symbol from index_tokens where symbol != 'WETH') b
	left join raw_price_1 c
	on b.asset = c.asset
	and c.block_time = a."hour"

)
,

 price_by_hour_1 as
(
    select
        block_time,
        asset,
		symbol,
        price,
        first_value(price) over (partition by asset,grp order by block_time ) as price_correct
    from (
          select
              block_time,
              asset,
			  symbol,
              price,
              sum(case when price is not null then 1 end) over (partition by asset order by block_time) as grp
          from raw_price_2 
          
         ) x
)
,
/*moving average the price*/
price_by_hour as 
(
    select
       block_time,
       asset,
       symbol,
       price_correct,
       avg(price_correct) over(partition by asset,symbol order by block_time ROWS BETWEEN 11 PRECEDING AND CURRENT ROW ) as price_ma
    from price_by_hour_1

),
--select * from price_by_hour
--where block_time in('2021-04-09 02:00','2021-04-09 01:00','2021-04-09 03:00');

index_nav as
(
    select
        a.price_time,
        sum(price_ma * asset_correct) as nav
    from index_composition_by_hour a
    left join price_by_hour b
    on a.contract_address = b.asset
      and a.price_time = b.block_time
    left join index_composition_in_out_time c
    on a.contract_address = c.asset_address
    where a.price_time >= c.in_index_time
      and a.price_time <= c.out_index_time
      and a.price_time >= (select start_time from start_time_table )
    group by a.price_time

)
/*test price and index composition*/
--select * from price_by_hour
--where block_time in('2021-05-31 05:00','2021-05-31 06:00','2021-05-31 07:00');
--select * from index_composition_by_hour
--where price_time in('2021-06-20 13:18' ,'2021-06-17 17:02');
--select * from index_nav;

select 
    price_time,
	nav as mvi_nav,
	price_ma as mvi_market_price,
	((price_ma - nav) / nav) AS premium_discount_percentage
from index_nav a
left join (select * from price_by_hour where symbol='MVI') b
on a.price_time = b.block_time
where nav is not null and price_correct is not null;
;

/*
price test
2021-04-06 06:00 0x6073ae636d6b809edffef419125f2b6055655bb9612ae554313c5a702ed81f58
2021-04-06 13:00 0x3ebe2d99d1318730f33e1129e816e83cd6473ad1142e9e98d2faa3fb3db25392

AXS abnormal: someone swap 99 eth for 1175 AXS
2021/5/31 6:00 https://etherscan.io/tx/0x411f856be7f00ff0e9ee3ac2f125483735dbe2095c2e23e534b0c3d68f4eb083
               https://etherscan.io/tx/0xccb28bd77e6f513f80194374743707e3749c945bba90b33b49d6255af00245aa
*/



