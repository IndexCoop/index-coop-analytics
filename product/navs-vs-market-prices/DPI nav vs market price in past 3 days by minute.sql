/* dune query here: https://duneanalytics.com/queries/58400
This query compares past 3 days DPI NAV and it's market price by minute.
How it works?
1.figures out the composition of per DPI each minute,then times the price of each compostion at that time ,adds them up gets the dpi nav
2.compares dpi nav with dpi market price 
*/

/*CREAM price from uniswap
CREAM/USDC pool address: 0xfb1833894E74Ebe68B8CCB02Ae2623B838b618aF
CREAM 0x2ba592F78dB6436527729929AAf6c908497cB200  decimals 18
USDC 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 decimals 6
*/

with cream_price_uni as
(
    select
        '\x2ba592F78dB6436527729929AAf6c908497cB200'::bytea as contract_address,
        date_trunc('minute', a."evt_block_time") as price_time,
        avg(case when a."contract_address" = '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' 
             then (a."value"/1e6)/(b."value"/1e18) 
             else (b."value"/1e6)/(a."value"/1e18)
        end) as price,
        'CREAM' as symbol
    from 
        (
	        select
                "evt_block_time",
                "evt_tx_hash",
                "contract_address",
                "value"
            from erc20."ERC20_evt_Transfer"
            where "to" = '\xfb1833894E74Ebe68B8CCB02Ae2623B838b618aF'
            and "contract_address" in ('\x2ba592F78dB6436527729929AAf6c908497cB200','\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48') 
        ) a
   inner join 
        (   
		    select
               "evt_block_time",
               "evt_tx_hash",
               "contract_address",
               "value"
            from erc20."ERC20_evt_Transfer"
            where "from" = '\xfb1833894E74Ebe68B8CCB02Ae2623B838b618aF'
            and "contract_address" in ('\x2ba592F78dB6436527729929AAf6c908497cB200','\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48') 
        ) b
    on a."evt_tx_hash" = b."evt_tx_hash"
    where  a."evt_block_time" > now() - interval '30 days'
    group by a."evt_block_time"
        )
,

cream_price_start_time as 
(
    select
        least(max(price_time), (select min(price_time) from cream_price_uni)) as price_start_time
    from cream_price_uni
    where price_time <= now() - interval '3 days'
)
,

time_windows as 
(
    select
        date_trunc('minute', x) as price_time
    from generate_series( (select price_start_time from cream_price_start_time),
                          now(),
                          interval  '1 min'
                        ) t(x)
)
,

cream_price_in_minute as
(
    select        
        contract_address,
        price_time,
        first_value(price) over (partition by grp_asset order by price_time ) as price,
        symbol
    from (
          select 
              '\x2ba592F78dB6436527729929AAf6c908497cB200'::bytea as contract_address,
              a.price_time,
              b.price,
              b.symbol,
              sum(case when b.price is not null then 1 end) over (order by a.price_time) as grp_asset
          from time_windows a 
          left join cream_price_uni b
          on a.price_time = b.price_time
          ) x
)
,

start_date_of_past_3_days as
(
    select
        max(minute_date) as start_date
    from (
           select
                "evt_tx_hash",
                "evt_block_time",
                date_trunc('minute', "evt_block_time") as minute_date,
                "_quantity"/1e18 as DPI_amount
           from setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
           where "_setToken" IN ('\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b')
             and "evt_block_time" <= now() - interval '3 days'
         ) x
)
,

DPI_mint_amount as
(
    select
        *
    from (
          select
              "evt_tx_hash",
              "evt_block_time",
              date_trunc('minute', "evt_block_time") as minute_date,
              "_quantity"/1e18 as DPI_amount,
              row_number() over(partition by date_trunc('minute', "evt_block_time") order by  "evt_block_time" desc) as rnb
          from setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
          where "_setToken" IN ('\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b')
         ) x 
    where x.rnb = 1
      and minute_date >= (select start_date from start_date_of_past_3_days)
)
,
/*DPI composition when minted*/
DPI_composition as
(
    select
        a."evt_tx_hash",
        a."evt_block_time",
        date_trunc('minute', a."evt_block_time") as minute_date,
        a."contract_address" as asset_address,
        a."value"/10^b."decimals"/c.DPI_amount as asset_amount,
        c.DPI_amount,
        b.symbol
    from (
          select
              *
          from erc20."ERC20_evt_Transfer"
          where "evt_tx_hash" in (select "evt_tx_hash" from DPI_mint_amount )
          and "to" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
          and  "contract_address" != '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
         )  a
    left join erc20."tokens" b
    on a."contract_address" = b."contract_address"
    left join DPI_mint_amount c
    on a."evt_tx_hash" = c."evt_tx_hash"
    
)
,
/*composition in and out of index time*/
DPI_composition_in_out_time as
(
    select
        asset_address,
        in_index_time,
        case when last_time < last_mint_time then last_time else now() end as out_index_time
    from (
          select
             asset_address,
             min(minute_date) as in_index_time,
             max(minute_date) as last_time,
             (select max(minute_date) from DPI_composition ) as last_mint_time
          from DPI_composition 
          group by asset_address
          ) x 

)
,

/*DPI composition prices by minute*/
DPI_composition_price as
(
    
    select 
        "contract_address" as contract_address,
        date_trunc('minute', "minute") as price_time,
        "price" as   price,
         symbol
    from prices."usd"
    where "contract_address" in (select distinct asset_address  from DPI_composition)
    and "minute" >= (select min(price_start_time) from cream_price_start_time)
    
    union all
    
    /*Cream price*/
    select
        contract_address,
        price_time,
        price,
        symbol
    from cream_price_in_minute
)
,

dpi_composition_time_series as
(
    select
        date_trunc('minute', x) as price_time
    from generate_series( (select min(minute_date) from DPI_composition),
                          now(),
                          interval  '1 min'
                         ) t(x)
),

/*DPI composition by minute*/
DPI_composition_by_minute as
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
          from dpi_composition_time_series a
          cross join (select distinct asset_address as contract_address from DPI_composition ) x
          left join  DPI_composition b
          on a.price_time = b.minute_date 
            and b.asset_address = x.contract_address
         ) x
)
,
/*DPI NAV*/
DPI_NAV_t as 
(
    select
        a.price_time,
        sum(price*asset_correct) as DPI_NAV
    from DPI_composition_price a
    left join DPI_composition_by_minute b
    on a.contract_address = b.contract_address
      and a.price_time = b.price_time
    left join DPI_composition_in_out_time c
    on a.contract_address = c.asset_address
    where a.price_time >= c.in_index_time
      and a.price_time <= c.out_index_time
    group by a.price_time
    
)
,

/*DPI market price*/
DPI_market_price_t as
(
    select
        date_trunc('minute', "minute") as minute_date,
        "price" as DPI_market_price
    from prices."usd"
    where "contract_address" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    and "minute" >= (select min(price_start_time) from cream_price_start_time)

)

select
    a.minute_date,
    a.DPI_market_price,
    b.DPI_NAV
from DPI_market_price_t a 
left join DPI_NAV_t b
on a.minute_date = b.price_time
where a.minute_date >= now() - interval '3 days'
order by a.minute_date;









