/* dune query here: https://duneanalytics.com/queries/53567
This query compares all time DPI NAV and it's market price hourly.
How it works?
1.figures out the composition of per DPI each hour,then times the price of each compostion at that time ,adds them up gets the dpi nav
2.compares dpi nav with dpi market price 
*/

/*DPI minted amount each time,only keeps one every hour*/
with DPI_mint_amount as
(
    select
        *
    from (
          select
              "evt_tx_hash",
              "evt_block_time",
              date_trunc('hour', "evt_block_time") as hour_date,
              "_quantity"/1e18 as DPI_amount,
              row_number() over(partition by date_trunc('hour', "evt_block_time") order by  "evt_block_time" desc) as rnb
          from setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
          where "_setToken" IN ('\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b')
         ) x 
    where x.rnb = 1
),

/*DPI composition when minted*/
DPI_composition as
(
    select
        a."evt_tx_hash",
        a."evt_block_time",
        date_trunc('hour', a."evt_block_time") as hour_date,
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
              min(hour_date) as in_index_time,
              max(hour_date) as last_time,
              (select max(hour_date) from DPI_composition ) as last_mint_time
          from DPI_composition 
          group by asset_address
         ) x 

),

/*DPI composition prices in hour*/
DPI_composition_price as
(
    
    select 
        "contract_address",
        date_trunc('hour', "minute") as price_time,
        avg("price") as  price,
        symbol
    from prices."usd"
    where "contract_address" in (select distinct asset_address  from DPI_composition)
    and "minute" >= (select min(hour_date) from DPI_mint_amount)
    group by "contract_address",date_trunc('hour', "minute"), symbol
    
    union all
    
    /*Cream has hour price data in  dex."view_token_prices" */
    select 
        "contract_address",
        "hour" as price_time,
        "median_price" as price,
        'CREAM' as symbol
    from dex."view_token_prices"
    where "contract_address" in ('\x2ba592f78db6436527729929aaf6c908497cb200')
    and  "hour" >= (select min(hour_date) from DPI_mint_amount)
)
,

/*DPI composition every hour nav:amount*price */
DPI_composition_hourly_NAV as
(
    select
        contract_address,
        price_time,
        price,
        asset_amount,
        first_value(asset_amount) over (partition by contract_address,grp_asset order by price_time ) as asset_correct,
        price*first_value(asset_amount) over (partition by contract_address,grp_asset order by price_time ) as asset_NAV
    from (
          select
              a.contract_address,
              a.price_time,
              a.price,
              b.asset_amount,
              sum(case when b.asset_amount is not null then 1 end) over (partition by a.contract_address order by a.price_time) as grp_asset
          from DPI_composition_price a
          left join DPI_composition b
          on a.contract_address = b.asset_address
          and a.price_time = b.hour_date
          left join DPI_composition_in_out_time c
          on a.contract_address = c.asset_address
          where a.price_time >= c.in_index_time
            and a.price_time <= c.out_index_time
         ) x
)
,

/*DPI NAV*/
DPI_NAV_t as 
(
    select
        price_time,
        sum(asset_NAV) as DPI_NAV
    from DPI_composition_hourly_NAV 
    group by price_time
    
)
,

/*DPI market price*/
DPI_market_price_t as
(
    select
        date_trunc('hour', "minute") as hour_date,
        avg("price") as DPI_market_price
    from prices."usd"
    where "contract_address" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    group by date_trunc('hour', "minute")

)

select
    a.hour_date,
    a.DPI_market_price,
    b.DPI_NAV
from DPI_market_price_t a 
left join DPI_NAV_t b
on a.hour_date = b.price_time
order by a.hour_date;