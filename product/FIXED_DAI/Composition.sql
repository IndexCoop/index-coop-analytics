with fixed_dai_mints as (
select * from erc20."ERC20_evt_Transfer"
where contract_address = '\xFB4D3b07aA16eE563Ea7C1f3202959448458e290'
and "from" = '\x0000000000000000000000000000000000000000'
--and "to" = '\x0000000000000000000000000000000000000000'
), 
fixed_dai_redeems as (
select * from erc20."ERC20_evt_Transfer"
where contract_address = '\xFB4D3b07aA16eE563Ea7C1f3202959448458e290'
and "to" = '\x0000000000000000000000000000000000000000'
--and "to" = '\x0000000000000000000000000000000000000000'
), 
mints_redeems as (
select * from  fixed_dai_mints
union all
select * from fixed_dai_redeems 
)
,
fcash_events as (select date_trunc('minute', evt_block_time) as minute, "netfCash"/1e8 as fCash_change, * from notional_v2."Router_evt_LendBorrowTrade"
where evt_tx_hash in (select evt_tx_hash from mints_redeems)
--and maturity = 1671840000
order by evt_block_time asc

--where evt_tx_hash = '\xca915595cbbcd99c8f091ca049dbadab12e8a901c9a2481f034de1e5f87c0305'
--where owner = '\x015558c3ab97c9e5a9c8c437c71bb498b2e5afb3'
), 
 maturities_minutes as (
select
    gs.minute,
    t.maturity
from        fcash_events t
inner join  (select generate_series((select min(minute) from fcash_events), date_trunc('minute', now()), '1 minute') as minute) gs on gs.minute >= (select min(minute) from fcash_events f where f.maturity = t.maturity)
--where t.symbol = 'ETH2x-FLI'
      ), 
      
minutes_fcash as (
select 
m.maturity, 
coalesce(avg(f.fCash_change),0) as fCash_change,  
--f.evt_block_time, 
m.minute--, 
/*sum(fCash_change) over (
        partition by m.maturity
        order by
          m.minute asc rows between unbounded preceding
          and current row
      ) as fCash_balance*/
from maturities_minutes m
left join fcash_events f on f.minute = m.minute and m.maturity = f.maturity
group by m.maturity, m.minute
)
select sum(fCash_change) over (
        partition by maturity
        order by
          minute asc rows between unbounded preceding
          and current row
      ) as fCash_balance, * 
      from minutes_fcash
/*()
select *, 
sum(fCash_change) over (
        partition by maturity
        order by
          minute asc rows between unbounded preceding
          and current row
      ) as fCash_balance
from minutes_fcash 
*/

      


